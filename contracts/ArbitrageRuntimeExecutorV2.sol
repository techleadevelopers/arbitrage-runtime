// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface IERC20MinimalV2 {
    function balanceOf(address owner) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 value) external returns (bool);
    function transfer(address to, uint256 value) external returns (bool);
}

interface IUniswapV2PairLikeV2 {
    function token0() external view returns (address);
    function token1() external view returns (address);
    function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    function swap(uint256 amount0Out, uint256 amount1Out, address to, bytes calldata data) external;
}

interface IUniswapV2RouterLikeV2 {
    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) external returns (uint256[] memory amounts);
}

contract ArbitrageRuntimeExecutorV2 {
    struct V2SwapStep {
        address router;
        address[] path;
        uint256 amountIn;
        uint256 minOut;
    }

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

    address public owner;
    mapping(address => bool) public authorizedOperators;
    mapping(address => bool) public allowedV2Pairs;
    mapping(address => bool) public allowedRouters;
    mapping(address => bool) public allowedBorrowTokens;
    bytes32 public activeExecutionId;
    bool public executionInProgress;

    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    event OperatorAuthorizationUpdated(address indexed operator, bool allowed);
    event V2PairAuthorizationUpdated(address indexed pair, bool allowed);
    event RouterAuthorizationUpdated(address indexed router, bool allowed);
    event BorrowTokenAuthorizationUpdated(address indexed token, bool allowed);
    event V2ExecutionStarted(
        address indexed initiator,
        address indexed pair,
        address indexed borrowToken,
        uint256 borrowAmount,
        uint256 minProfit,
        address profitToken
    );
    event V2StepExecuted(
        uint256 indexed stepIndex,
        address indexed router,
        address tokenIn,
        address tokenOut,
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
    error UnsupportedPair();
    error UnsupportedBorrowToken();
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

    function setAllowedV2Pair(address pair, bool allowed) external onlyOwner {
        if (pair == address(0)) revert ZeroAddress();
        allowedV2Pairs[pair] = allowed;
        emit V2PairAuthorizationUpdated(pair, allowed);
    }

    function setAllowedV2Pairs(address[] calldata pairs, bool allowed) external onlyOwner {
        for (uint256 i = 0; i < pairs.length; ++i) {
            address pair = pairs[i];
            if (pair == address(0)) revert ZeroAddress();
            allowedV2Pairs[pair] = allowed;
            emit V2PairAuthorizationUpdated(pair, allowed);
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

    function isV2ExecutionAllowed(
        address operator,
        address pair,
        address borrowToken,
        address[] calldata routers
    ) external view returns (bool allowed, uint8 reasonCode) {
        if (!authorizedOperators[operator]) return (false, 1);
        if (!allowedV2Pairs[pair]) return (false, 2);
        if (!allowedBorrowTokens[borrowToken]) return (false, 3);
        for (uint256 i = 0; i < routers.length; ++i) {
            if (!allowedRouters[routers[i]]) return (false, 4);
        }
        return (true, 0);
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

        address token0 = IUniswapV2PairLikeV2(pair).token0();
        address token1 = IUniswapV2PairLikeV2(pair).token1();
        if (borrowToken != token0 && borrowToken != token1) revert UnsupportedBorrowToken();

        bytes32 executionId = _deriveExecutionId(
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
        IUniswapV2PairLikeV2(pair).swap(amount0Out, amount1Out, address(this), abi.encode(ctx));
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
            (address tokenIn, address tokenOut, uint256 amountOut) = _executeV2Step(ctx.steps[i], i);
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

    function _executeV2Step(
        V2SwapStep memory step,
        uint256 stepIndex
    ) internal returns (address tokenIn, address tokenOut, uint256 amountOut) {
        tokenIn = step.path[0];
        tokenOut = step.path[step.path.length - 1];

        uint256 amountIn = _currentAmountIn(step.amountIn, tokenIn);
        if (amountIn == 0) revert ZeroAmount();

        _forceApprove(tokenIn, step.router, amountIn);
        uint256 beforeOut = _balanceOf(tokenOut);
        uint256[] memory amounts = IUniswapV2RouterLikeV2(step.router).swapExactTokensForTokens(
            amountIn,
            step.minOut,
            step.path,
            address(this),
            block.timestamp
        );
        if (amounts.length == 0) revert StepExecutionFailed(stepIndex);
        amountOut = amounts[amounts.length - 1];

        uint256 afterOut = _balanceOf(tokenOut);
        if (afterOut < beforeOut) revert StepExecutionFailed(stepIndex);
        uint256 balanceDiff = afterOut - beforeOut;
        if (balanceDiff > amountOut) amountOut = balanceDiff;
        if (amountOut < step.minOut) revert StepExecutionFailed(stepIndex);
    }

    function _settleV2PairDebt(
        V2CallbackContext memory ctx,
        uint256 amount0,
        uint256 amount1
    ) internal returns (uint256 repaymentAmount) {
        uint256 borrowedAmount = amount0 > 0 ? amount0 : amount1;
        if (borrowedAmount == 0) borrowedAmount = ctx.borrowAmount;
        repaymentAmount = _v2RepaymentAmountIn(ctx.pair, ctx.borrowToken, ctx.profitToken, borrowedAmount);
        _transferToken(ctx.profitToken, ctx.pair, repaymentAmount);
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

    function _deriveExecutionId(
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
                keccak256("V2"),
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
        return IERC20MinimalV2(token).balanceOf(address(this));
    }

    function _transferToken(address token, address to, uint256 amount) internal {
        if (amount == 0) return;
        bool ok = IERC20MinimalV2(token).transfer(to, amount);
        if (!ok) revert RepaymentFailed();
    }

    function _forceApprove(address token, address spender, uint256 amount) internal {
        IERC20MinimalV2 erc20 = IERC20MinimalV2(token);
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

    function _v2RepaymentAmount(uint256 borrowedAmount) internal pure returns (uint256) {
        return ((borrowedAmount * 1000) / 997) + 1;
    }

    function _v2RepaymentAmountIn(
        address pair,
        address borrowedToken,
        address repaymentToken,
        uint256 borrowedAmount
    ) internal view returns (uint256) {
        if (repaymentToken == borrowedToken) return _v2RepaymentAmount(borrowedAmount);

        address token0 = IUniswapV2PairLikeV2(pair).token0();
        address token1 = IUniswapV2PairLikeV2(pair).token1();
        if (
            !((borrowedToken == token0 && repaymentToken == token1) ||
                (borrowedToken == token1 && repaymentToken == token0))
        ) {
            revert UnsupportedBorrowToken();
        }

        (uint112 reserve0, uint112 reserve1, ) = IUniswapV2PairLikeV2(pair).getReserves();
        uint256 reserveIn = repaymentToken == token0 ? uint256(reserve0) : uint256(reserve1);
        uint256 reserveOut = borrowedToken == token0 ? uint256(reserve0) : uint256(reserve1);
        if (borrowedAmount >= reserveOut) revert RepaymentFailed();

        uint256 numerator = reserveIn * borrowedAmount * 1000;
        uint256 denominator = (reserveOut - borrowedAmount) * 997;
        return (numerator / denominator) + 1;
    }
}
