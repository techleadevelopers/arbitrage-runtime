// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface IERC20MinimalV3Only {
    function balanceOf(address owner) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 value) external returns (bool);
    function transfer(address to, uint256 value) external returns (bool);
}

interface IUniswapV3PoolLikeV3Only {
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

interface IUniswapV3RouterLikeV3Only {
    struct ExactInputParams {
        bytes path;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
    }

    function exactInput(ExactInputParams calldata params) external payable returns (uint256 amountOut);
}

contract ArbitrageRuntimeExecutorV3 {
    uint160 private constant MIN_SQRT_RATIO_PLUS_ONE = 4295128740;
    uint160 private constant MAX_SQRT_RATIO_MINUS_ONE = 1461446703485210103287273052203988822378723970341;

    struct V3SwapStep {
        address router;
        bytes path;
        uint256 amountIn;
        uint256 minOut;
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

    address public owner;
    mapping(address => bool) public authorizedOperators;
    mapping(address => bool) public allowedV3Pools;
    mapping(address => bool) public allowedRouters;
    mapping(address => bool) public allowedBorrowTokens;
    mapping(uint24 => bool) public allowedFeeTiers;
    bytes32 public activeExecutionId;
    bool public executionInProgress;

    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    event OperatorAuthorizationUpdated(address indexed operator, bool allowed);
    event V3PoolAuthorizationUpdated(address indexed pool, bool allowed);
    event RouterAuthorizationUpdated(address indexed router, bool allowed);
    event BorrowTokenAuthorizationUpdated(address indexed token, bool allowed);
    event FeeTierAuthorizationUpdated(uint24 indexed feeTier, bool allowed);
    event V3ExecutionStarted(
        address indexed initiator,
        address indexed pool,
        address indexed borrowToken,
        uint24 feeTier,
        uint256 borrowAmount,
        uint256 minProfit,
        address profitToken
    );
    event V3StepExecuted(
        uint256 indexed stepIndex,
        address indexed router,
        bytes path,
        uint256 amountIn,
        uint256 amountOut
    );
    event ExecutionSettled(
        bytes32 indexed executionId,
        address indexed profitToken,
        uint256 grossProfit,
        uint256 netProfit,
        uint256 repaymentAmount
    );
    event TokenRescued(address indexed token, address indexed to, uint256 amount);
    event NativeRescued(address indexed to, uint256 amount);

    error OnlyOwner();
    error OnlyAuthorizedOperator();
    error ZeroAddress();
    error ZeroAmount();
    error InvalidPath();
    error InvalidStep();
    error InvalidCaller();
    error UnsupportedPool();
    error UnsupportedBorrowToken();
    error UnsupportedFeeTier();
    error ExecutionAlreadyInProgress();
    error NoExecutionInProgress();
    error UnknownExecution();
    error InvalidExecutionSource();
    error StepExecutionFailed(uint256 stepIndex);
    error RepaymentFailed();
    error ProfitBelowMinimum(uint256 realizedProfit, uint256 minimumProfit);

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

    receive() external payable {}

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

    function setAllowedV3Pool(address pool, bool allowed) external onlyOwner {
        if (pool == address(0)) revert ZeroAddress();
        allowedV3Pools[pool] = allowed;
        emit V3PoolAuthorizationUpdated(pool, allowed);
    }

    function setAllowedV3Pools(address[] calldata pools, bool allowed) external onlyOwner {
        for (uint256 i = 0; i < pools.length; ++i) {
            address pool = pools[i];
            if (pool == address(0)) revert ZeroAddress();
            allowedV3Pools[pool] = allowed;
            emit V3PoolAuthorizationUpdated(pool, allowed);
        }
    }

    function setAllowedRouter(address router, bool allowed) external onlyOwner {
        if (router == address(0)) revert ZeroAddress();
        allowedRouters[router] = allowed;
        emit RouterAuthorizationUpdated(router, allowed);
    }

    function setAllowedRouters(address[] calldata routers, bool allowed) external onlyOwner {
        for (uint256 i = 0; i < routers.length; ++i) {
            address router = routers[i];
            if (router == address(0)) revert ZeroAddress();
            allowedRouters[router] = allowed;
            emit RouterAuthorizationUpdated(router, allowed);
        }
    }

    function setAllowedBorrowToken(address token, bool allowed) external onlyOwner {
        if (token == address(0)) revert ZeroAddress();
        allowedBorrowTokens[token] = allowed;
        emit BorrowTokenAuthorizationUpdated(token, allowed);
    }

    function setAllowedBorrowTokens(address[] calldata tokens, bool allowed) external onlyOwner {
        for (uint256 i = 0; i < tokens.length; ++i) {
            address token = tokens[i];
            if (token == address(0)) revert ZeroAddress();
            allowedBorrowTokens[token] = allowed;
            emit BorrowTokenAuthorizationUpdated(token, allowed);
        }
    }

    function setAllowedFeeTier(uint24 feeTier, bool allowed) external onlyOwner {
        allowedFeeTiers[feeTier] = allowed;
        emit FeeTierAuthorizationUpdated(feeTier, allowed);
    }

    function setAllowedFeeTiers(uint24[] calldata feeTiers, bool allowed) external onlyOwner {
        for (uint256 i = 0; i < feeTiers.length; ++i) {
            uint24 feeTier = feeTiers[i];
            allowedFeeTiers[feeTier] = allowed;
            emit FeeTierAuthorizationUpdated(feeTier, allowed);
        }
    }

    function rescueToken(address token, address to, uint256 amount) external onlyOwner {
        if (token == address(0) || to == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();
        _transferToken(token, to, amount);
        emit TokenRescued(token, to, amount);
    }

    function rescueNative(address payable to, uint256 amount) external onlyOwner {
        if (to == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();
        if (amount > address(this).balance) revert RepaymentFailed();
        (bool ok, ) = to.call{value: amount}("");
        if (!ok) revert RepaymentFailed();
        emit NativeRescued(to, amount);
    }

    function isV3ExecutionAllowed(
        address operator,
        address pool,
        address borrowToken,
        uint24 feeTier,
        address[] calldata routers
    ) external view returns (bool allowed, uint8 reasonCode) {
        if (!authorizedOperators[operator]) return (false, 1);
        if (!allowedV3Pools[pool]) return (false, 2);
        if (!allowedBorrowTokens[borrowToken]) return (false, 3);
        if (!allowedFeeTiers[feeTier]) return (false, 4);
        for (uint256 i = 0; i < routers.length; ++i) {
            if (!allowedRouters[routers[i]]) return (false, 5);
        }
        return (true, 0);
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

        address token0 = IUniswapV3PoolLikeV3Only(pool).token0();
        address token1 = IUniswapV3PoolLikeV3Only(pool).token1();
        if (borrowToken != token0 && borrowToken != token1) revert UnsupportedBorrowToken();

        bytes32 executionId = _deriveExecutionId(
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

        emit V3ExecutionStarted(msg.sender, pool, borrowToken, feeTier, borrowAmount, minProfit, profitToken);
        _startV3PoolSwap(pool, borrowToken == token1, borrowAmount, ctx);
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
            uint256 amountOut = _executeV3Step(ctx.steps[i], i);
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

    function _executeV3Step(V3SwapStep memory step, uint256 stepIndex) internal returns (uint256 amountOut) {
        address tokenIn = _v3PathTokenIn(step.path);
        address tokenOut = _v3PathTokenOut(step.path);
        uint256 amountIn = _currentAmountIn(step.amountIn, tokenIn);
        if (amountIn == 0) revert ZeroAmount();

        _forceApprove(tokenIn, step.router, amountIn);
        uint256 beforeOut = _balanceOf(tokenOut);
        amountOut = IUniswapV3RouterLikeV3Only(step.router).exactInput(
            IUniswapV3RouterLikeV3Only.ExactInputParams({
                path: step.path,
                recipient: address(this),
                deadline: block.timestamp,
                amountIn: amountIn,
                amountOutMinimum: step.minOut
            })
        );

        uint256 afterOut = _balanceOf(tokenOut);
        if (afterOut < beforeOut) revert StepExecutionFailed(stepIndex);
        uint256 balanceDiff = afterOut - beforeOut;
        if (balanceDiff > amountOut) amountOut = balanceDiff;
        if (amountOut < step.minOut) revert StepExecutionFailed(stepIndex);
    }

    function _settleV3PoolDebt(
        V3CallbackContext memory ctx,
        int256 amount0Delta,
        int256 amount1Delta
    ) internal returns (uint256 repaymentAmount) {
        address token0 = IUniswapV3PoolLikeV3Only(ctx.pool).token0();
        address token1 = IUniswapV3PoolLikeV3Only(ctx.pool).token1();

        if (amount0Delta > 0) {
            uint256 owed0 = uint256(amount0Delta);
            _transferToken(token0, ctx.pool, owed0);
            repaymentAmount += owed0;
        }
        if (amount1Delta > 0) {
            uint256 owed1 = uint256(amount1Delta);
            _transferToken(token1, ctx.pool, owed1);
            repaymentAmount += owed1;
        }
    }

    function _beginExecution(bytes32 executionId) internal {
        if (executionInProgress) revert ExecutionAlreadyInProgress();
        executionInProgress = true;
        activeExecutionId = executionId;
    }

    function _startV3PoolSwap(
        address pool,
        bool zeroForOne,
        uint256 borrowAmount,
        V3CallbackContext memory ctx
    ) internal {
        uint160 sqrtPriceLimitX96 = zeroForOne ? MIN_SQRT_RATIO_PLUS_ONE : MAX_SQRT_RATIO_MINUS_ONE;
        IUniswapV3PoolLikeV3Only(pool).swap(
            address(this),
            zeroForOne,
            -int256(borrowAmount),
            sqrtPriceLimitX96,
            abi.encode(ctx)
        );
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
        address pool,
        address borrowToken,
        uint256 borrowAmount,
        uint256 minProfit,
        address profitToken,
        address profitRecipient,
        uint256 stepCount
    ) internal view returns (bytes32) {
        return keccak256(
            abi.encodePacked(
                keccak256("V3"),
                block.chainid,
                address(this),
                pool,
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
        return IERC20MinimalV3Only(token).balanceOf(address(this));
    }

    function _transferToken(address token, address to, uint256 amount) internal {
        if (amount == 0) return;
        bool ok = IERC20MinimalV3Only(token).transfer(to, amount);
        if (!ok) revert RepaymentFailed();
    }

    function _forceApprove(address token, address spender, uint256 amount) internal {
        IERC20MinimalV3Only erc20 = IERC20MinimalV3Only(token);
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
        if (requestedAmount == type(uint256).max) return _balanceOf(tokenIn);
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
}
