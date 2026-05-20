// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {IArbitrageRuntimeExecutor} from "../interfaces/IArbitrageRuntimeExecutor.sol";

interface IERC20Minimal {
    function balanceOf(address owner) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 value) external returns (bool);
    function transfer(address to, uint256 value) external returns (bool);
}

interface IUniswapV2PairLike {
    function token0() external view returns (address);
    function token1() external view returns (address);
    function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    function swap(uint256 amount0Out, uint256 amount1Out, address to, bytes calldata data) external;
}

interface IUniswapV3PoolLike {
    function token0() external view returns (address);
    function token1() external view returns (address);
    function swap(
        address recipient,
        bool zeroForOne,
        int256 amountSpecified,
        uint160 sqrtPriceLimitX96,
        bytes calldata data
    ) external returns (int256 amount0, int256 amount1);
}

interface IUniswapV2RouterLike {
    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) external returns (uint256[] memory amounts);
}

interface IUniswapV3RouterLike {
    struct ExactInputParams {
        bytes path;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
    }

    function exactInput(ExactInputParams calldata params) external payable returns (uint256 amountOut);
}

abstract contract ArbitrageRuntimeExecutorBase is IArbitrageRuntimeExecutor {
    uint160 internal constant MIN_SQRT_RATIO_PLUS_ONE = 4295128740;
    uint160 internal constant MAX_SQRT_RATIO_MINUS_ONE =
        1461446703485210103287273052203988822378723970341;

    address public owner;
    mapping(address => bool) public authorizedOperators;
    mapping(address => bool) public allowedV2Pairs;
    mapping(address => bool) public allowedV3Pools;
    mapping(address => bool) public allowedRouters;
    mapping(address => bool) public allowedBorrowTokens;
    mapping(uint24 => bool) public allowedFeeTiers;

    bytes32 public activeExecutionId;
    bool public executionInProgress;

    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    event OperatorAuthorizationUpdated(address indexed operator, bool allowed);
    event V2PairAuthorizationUpdated(address indexed pair, bool allowed);
    event V3PoolAuthorizationUpdated(address indexed pool, bool allowed);
    event RouterAuthorizationUpdated(address indexed router, bool allowed);
    event BorrowTokenAuthorizationUpdated(address indexed token, bool allowed);
    event FeeTierAuthorizationUpdated(uint24 indexed feeTier, bool allowed);

    error OnlyOwner();
    error OnlyAuthorizedOperator();
    error ExecutionAlreadyInProgress();
    error NoExecutionInProgress();
    error UnknownExecution();
    error InvalidExecutionSource();
    struct V2CallbackContext {
        bytes32 executionId;
        address pair;
        address borrowToken;
        uint256 borrowAmount;
        uint256 minProfit;
        address profitToken;
        address profitRecipient;
        V2SwapStep[] steps;
    }

    struct V3CallbackContext {
        bytes32 executionId;
        address pool;
        address borrowToken;
        uint256 borrowAmount;
        uint24 feeTier;
        uint256 minProfit;
        address profitToken;
        address profitRecipient;
        V3SwapStep[] steps;
    }

    modifier onlyOwner() {
        if (msg.sender != owner) revert OnlyOwner();
        _;
    }

    modifier onlyAuthorizedOperator() {
        if (!authorizedOperators[msg.sender]) revert OnlyAuthorizedOperator();
        _;
    }

    constructor(address initialOwner) {
        if (initialOwner == address(0)) revert ZeroAddress();
        owner = initialOwner;
        authorizedOperators[initialOwner] = true;
        emit OwnershipTransferred(address(0), initialOwner);
        emit OperatorAuthorizationUpdated(initialOwner, true);
    }

    function transferOwnership(address newOwner) external onlyOwner {
        if (newOwner == address(0)) revert ZeroAddress();
        address previous = owner;
        owner = newOwner;
        authorizedOperators[newOwner] = true;
        emit OwnershipTransferred(previous, newOwner);
        emit OperatorAuthorizationUpdated(newOwner, true);
    }

    function setAuthorizedOperator(address operator, bool allowed) external onlyOwner {
        if (operator == address(0)) revert ZeroAddress();
        authorizedOperators[operator] = allowed;
        emit OperatorAuthorizationUpdated(operator, allowed);
    }

    function setAllowedV2Pair(address pair, bool allowed) external onlyOwner {
        if (pair == address(0)) revert ZeroAddress();
        allowedV2Pairs[pair] = allowed;
        emit V2PairAuthorizationUpdated(pair, allowed);
    }

    function setAllowedV3Pool(address pool, bool allowed) external onlyOwner {
        if (pool == address(0)) revert ZeroAddress();
        allowedV3Pools[pool] = allowed;
        emit V3PoolAuthorizationUpdated(pool, allowed);
    }

    function setAllowedRouter(address router, bool allowed) external onlyOwner {
        if (router == address(0)) revert ZeroAddress();
        allowedRouters[router] = allowed;
        emit RouterAuthorizationUpdated(router, allowed);
    }

    function setAllowedBorrowToken(address token, bool allowed) external onlyOwner {
        if (token == address(0)) revert ZeroAddress();
        allowedBorrowTokens[token] = allowed;
        emit BorrowTokenAuthorizationUpdated(token, allowed);
    }

    function setAllowedFeeTier(uint24 feeTier, bool allowed) external onlyOwner {
        allowedFeeTiers[feeTier] = allowed;
        emit FeeTierAuthorizationUpdated(feeTier, allowed);
    }

    function startV2FlashSwap(
        address pair,
        address borrowToken,
        uint256 borrowAmount,
        uint256 minProfit,
        address profitToken,
        address profitRecipient,
        V2SwapStep[] calldata steps
    ) external onlyAuthorizedOperator {
        if (pair == address(0) || borrowToken == address(0) || profitToken == address(0) || profitRecipient == address(0)) {
            revert ZeroAddress();
        }
        if (borrowAmount == 0) revert ZeroAmount();
        if (!allowedV2Pairs[pair]) revert UnsupportedPair();
        if (!allowedBorrowTokens[borrowToken]) revert UnsupportedBorrowToken();
        _validateV2Steps(steps);

        address token0 = IUniswapV2PairLike(pair).token0();
        address token1 = IUniswapV2PairLike(pair).token1();
        if (borrowToken != token0 && borrowToken != token1) revert UnsupportedBorrowToken();

        bytes32 executionId = _deriveExecutionId(
            keccak256("V2"),
            pair,
            borrowToken,
            borrowAmount,
            minProfit,
            profitToken,
            profitRecipient,
            steps.length
        );
        _beginExecution(executionId);

        V2CallbackContext memory ctx = V2CallbackContext({
            executionId: executionId,
            pair: pair,
            borrowToken: borrowToken,
            borrowAmount: borrowAmount,
            minProfit: minProfit,
            profitToken: profitToken,
            profitRecipient: profitRecipient,
            steps: steps
        });

        emit V2ExecutionStarted(msg.sender, pair, borrowToken, borrowAmount, minProfit, profitToken);

        uint256 amount0Out = borrowToken == token0 ? borrowAmount : 0;
        uint256 amount1Out = borrowToken == token1 ? borrowAmount : 0;
        IUniswapV2PairLike(pair).swap(amount0Out, amount1Out, address(this), abi.encode(ctx));
    }

    function startV3FlashSwap(
        address pool,
        address borrowToken,
        uint256 borrowAmount,
        uint24 feeTier,
        uint256 minProfit,
        address profitToken,
        address profitRecipient,
        V3SwapStep[] calldata steps
    ) external onlyAuthorizedOperator {
        if (pool == address(0) || borrowToken == address(0) || profitToken == address(0) || profitRecipient == address(0)) {
            revert ZeroAddress();
        }
        if (borrowAmount == 0) revert ZeroAmount();
        if (!allowedV3Pools[pool]) revert UnsupportedPool();
        if (!allowedBorrowTokens[borrowToken]) revert UnsupportedBorrowToken();
        if (!allowedFeeTiers[feeTier]) revert UnsupportedFeeTier();
        _validateV3Steps(steps);

        address token0 = IUniswapV3PoolLike(pool).token0();
        address token1 = IUniswapV3PoolLike(pool).token1();
        if (borrowToken != token0 && borrowToken != token1) revert UnsupportedBorrowToken();

        bytes32 executionId = _deriveExecutionId(
            keccak256("V3"),
            pool,
            borrowToken,
            borrowAmount,
            minProfit,
            profitToken,
            profitRecipient,
            steps.length
        );
        _beginExecution(executionId);

        V3CallbackContext memory ctx = V3CallbackContext({
            executionId: executionId,
            pool: pool,
            borrowToken: borrowToken,
            borrowAmount: borrowAmount,
            feeTier: feeTier,
            minProfit: minProfit,
            profitToken: profitToken,
            profitRecipient: profitRecipient,
            steps: steps
        });

        emit V3ExecutionStarted(
            msg.sender,
            pool,
            borrowToken,
            feeTier,
            borrowAmount,
            minProfit,
            profitToken
        );

        bool zeroForOne = borrowToken == token1;
        uint160 sqrtPriceLimitX96 = zeroForOne ? MIN_SQRT_RATIO_PLUS_ONE : MAX_SQRT_RATIO_MINUS_ONE;
        IUniswapV3PoolLike(pool).swap(
            address(this),
            zeroForOne,
            -int256(borrowAmount),
            sqrtPriceLimitX96,
            abi.encode(ctx)
        );
    }

    function uniswapV2Call(
        address sender,
        uint256 amount0,
        uint256 amount1,
        bytes calldata data
    ) external {
        if (!executionInProgress) revert NoExecutionInProgress();
        if (sender != address(this)) revert InvalidCaller();

        V2CallbackContext memory ctx = abi.decode(data, (V2CallbackContext));
        if (ctx.executionId != activeExecutionId) revert UnknownExecution();
        if (msg.sender != ctx.pair) revert InvalidExecutionSource();

        uint256 preProfitBalance = _balanceOf(ctx.profitToken);
        for (uint256 i = 0; i < ctx.steps.length; ++i) {
            (address tokenIn, address tokenOut, uint256 amountOut) = _executeV2Step(ctx.steps[i], i, ctx);
            emit V2StepExecuted(i, ctx.steps[i].router, tokenIn, tokenOut, ctx.steps[i].amountIn, amountOut);
        }

        uint256 repaymentAmount = _settleV2PairDebt(ctx, amount0, amount1);
        uint256 postProfitBalance = _balanceOf(ctx.profitToken);
        uint256 grossProfit = postProfitBalance > preProfitBalance ? postProfitBalance - preProfitBalance : 0;
        uint256 netProfit = _finalizeExecution(
            ctx.executionId,
            ctx.profitToken,
            ctx.profitRecipient,
            grossProfit,
            repaymentAmount
        );
        if (netProfit < ctx.minProfit) revert ProfitBelowMinimum(netProfit, ctx.minProfit);
    }

    function uniswapV3SwapCallback(
        int256 amount0Delta,
        int256 amount1Delta,
        bytes calldata data
    ) external {
        if (!executionInProgress) revert NoExecutionInProgress();

        V3CallbackContext memory ctx = abi.decode(data, (V3CallbackContext));
        if (ctx.executionId != activeExecutionId) revert UnknownExecution();
        if (msg.sender != ctx.pool) revert InvalidExecutionSource();

        uint256 preProfitBalance = _balanceOf(ctx.profitToken);
        for (uint256 i = 0; i < ctx.steps.length; ++i) {
            uint256 amountOut = _executeV3Step(ctx.steps[i], i, ctx);
            emit V3StepExecuted(i, ctx.steps[i].router, ctx.steps[i].path, ctx.steps[i].amountIn, amountOut);
        }

        uint256 repaymentAmount = _settleV3PoolDebt(ctx, amount0Delta, amount1Delta);
        uint256 postProfitBalance = _balanceOf(ctx.profitToken);
        uint256 grossProfit = postProfitBalance > preProfitBalance ? postProfitBalance - preProfitBalance : 0;
        uint256 netProfit = _finalizeExecution(
            ctx.executionId,
            ctx.profitToken,
            ctx.profitRecipient,
            grossProfit,
            repaymentAmount
        );
        if (netProfit < ctx.minProfit) revert ProfitBelowMinimum(netProfit, ctx.minProfit);
    }

    function _beginExecution(bytes32 executionId) internal {
        if (executionInProgress) revert ExecutionAlreadyInProgress();
        executionInProgress = true;
        activeExecutionId = executionId;
    }

    function _finalizeExecution(
        bytes32 executionId,
        address profitToken,
        address profitRecipient,
        uint256 grossProfit,
        uint256 repaymentAmount
    ) internal returns (uint256 netProfit) {
        netProfit = grossProfit;
        if (netProfit > 0 && profitRecipient != address(this)) {
            _transferToken(profitToken, profitRecipient, netProfit);
        }
        emit ExecutionSettled(executionId, profitToken, grossProfit, netProfit, repaymentAmount);
        executionInProgress = false;
        activeExecutionId = bytes32(0);
    }

    function _validateV2Steps(V2SwapStep[] calldata steps) internal view {
        if (steps.length == 0) revert InvalidStep();
        for (uint256 i = 0; i < steps.length; ++i) {
            V2SwapStep calldata step = steps[i];
            if (!allowedRouters[step.router]) revert InvalidStep();
            if (step.path.length < 2) revert InvalidPath();
            if (step.minOut == 0) revert ZeroAmount();
        }
    }

    function _validateV3Steps(V3SwapStep[] calldata steps) internal view {
        if (steps.length == 0) revert InvalidStep();
        for (uint256 i = 0; i < steps.length; ++i) {
            V3SwapStep calldata step = steps[i];
            if (!allowedRouters[step.router]) revert InvalidStep();
            if (step.path.length < 43) revert InvalidPath();
            if (step.minOut == 0) revert ZeroAmount();
        }
    }

    function _deriveExecutionId(
        bytes32 flowKind,
        address venue,
        address borrowToken,
        uint256 borrowAmount,
        uint256 minProfit,
        address profitToken,
        address profitRecipient,
        uint256 stepCount
    ) internal view returns (bytes32) {
        return keccak256(
            abi.encodePacked(
                flowKind,
                block.chainid,
                address(this),
                venue,
                borrowToken,
                borrowAmount,
                minProfit,
                profitToken,
                profitRecipient,
                stepCount,
                block.number
            )
        );
    }

    function _balanceOf(address token) internal view returns (uint256) {
        return IERC20Minimal(token).balanceOf(address(this));
    }

    function _transferToken(address token, address to, uint256 amount) internal {
        if (amount == 0) return;
        bool ok = IERC20Minimal(token).transfer(to, amount);
        if (!ok) revert RepaymentFailed();
    }

    function _forceApprove(address token, address spender, uint256 amount) internal {
        IERC20Minimal erc20 = IERC20Minimal(token);
        uint256 current = erc20.allowance(address(this), spender);
        if (current >= amount) return;
        if (current != 0) {
            bool resetOk = erc20.approve(spender, 0);
            if (!resetOk) revert StepExecutionFailed(type(uint256).max);
        }
        bool ok = erc20.approve(spender, amount);
        if (!ok) revert StepExecutionFailed(type(uint256).max);
    }

    function _currentAmountIn(uint256 requestedAmount, address tokenIn) internal view returns (uint256) {
        if (requestedAmount == type(uint256).max) {
            return _balanceOf(tokenIn);
        }
        return requestedAmount;
    }

    function _v3PathTokenIn(bytes memory path) internal pure returns (address tokenIn) {
        if (path.length < 43) revert InvalidPath();
        assembly {
            tokenIn := shr(96, mload(add(path, 32)))
        }
    }

    function _v3PathTokenOut(bytes memory path) internal pure returns (address tokenOut) {
        if (path.length < 43) revert InvalidPath();
        uint256 offset = path.length - 20;
        assembly {
            tokenOut := shr(96, mload(add(add(path, 32), offset)))
        }
    }

    function _v2RepaymentAmount(uint256 borrowedAmount) internal pure returns (uint256) {
        return ((borrowedAmount * 1000) / 997) + 1;
    }

    function _v2RepaymentAmountIn(
        address pair,
        address borrowedToken,
        address repaymentToken,
        uint256 borrowedAmount
    ) internal view returns (uint256) {
        if (repaymentToken == borrowedToken) {
            return _v2RepaymentAmount(borrowedAmount);
        }

        address token0 = IUniswapV2PairLike(pair).token0();
        address token1 = IUniswapV2PairLike(pair).token1();
        if (
            !((borrowedToken == token0 && repaymentToken == token1) ||
                (borrowedToken == token1 && repaymentToken == token0))
        ) {
            revert UnsupportedBorrowToken();
        }

        (uint112 reserve0, uint112 reserve1, ) = IUniswapV2PairLike(pair).getReserves();
        uint256 reserveIn = repaymentToken == token0 ? uint256(reserve0) : uint256(reserve1);
        uint256 reserveOut = borrowedToken == token0 ? uint256(reserve0) : uint256(reserve1);
        if (borrowedAmount >= reserveOut) revert RepaymentFailed();

        uint256 numerator = reserveIn * borrowedAmount * 1000;
        uint256 denominator = (reserveOut - borrowedAmount) * 997;
        return (numerator / denominator) + 1;
    }

    function _executeV2Step(
        V2SwapStep memory step,
        uint256 stepIndex,
        V2CallbackContext memory ctx
    ) internal virtual returns (address tokenIn, address tokenOut, uint256 amountOut);

    function _executeV3Step(
        V3SwapStep memory step,
        uint256 stepIndex,
        V3CallbackContext memory ctx
    ) internal virtual returns (uint256 amountOut);

    function _settleV2PairDebt(
        V2CallbackContext memory ctx,
        uint256 amount0,
        uint256 amount1
    ) internal virtual returns (uint256 repaymentAmount);

    function _settleV3PoolDebt(
        V3CallbackContext memory ctx,
        int256 amount0Delta,
        int256 amount1Delta
    ) internal virtual returns (uint256 repaymentAmount);
}
