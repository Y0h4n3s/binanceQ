from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

CLOSE_POLICY_BREAKEVEN: ClosePolicy
CLOSE_POLICY_BREAKEVEN_OR_MARKET_CLOSE: ClosePolicy
CLOSE_POLICY_IMMEDIATE_MARKET: ClosePolicy
CLOSE_POLICY_UNSPECIFIED: ClosePolicy
DESCRIPTOR: _descriptor.FileDescriptor
EXCHANGE_ID_BINANCE: ExchangeId
EXCHANGE_ID_SIMULATED: ExchangeId
ORDER_TYPE_LIMIT: OrderType
ORDER_TYPE_MARKET: OrderType
ORDER_TYPE_STOP_LOSS: OrderType
ORDER_TYPE_TAKE_PROFIT: OrderType
ORDER_TYPE_TRAILING_STOP_LOSS: OrderType
ORDER_TYPE_UNSPECIFIED: OrderType
SIDE_BUY: Side
SIDE_SELL: Side
SIDE_UNSPECIFIED: Side
SIGNAL_SOURCE_CHANGE_POINT_DETECTOR: SignalSource
SIGNAL_SOURCE_UNSPECIFIED: SignalSource

class NotifyResponse(_message.Message):
    __slots__ = ["message", "success"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    message: str
    success: bool
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class NotifySignalResponse(_message.Message):
    __slots__ = ["confirmed"]
    CONFIRMED_FIELD_NUMBER: _ClassVar[int]
    confirmed: bool
    def __init__(self, confirmed: bool = ...) -> None: ...

class Order(_message.Message):
    __slots__ = ["close_policy", "extra", "for_id", "id", "lifetime", "order_type", "price", "quantity", "side", "symbol", "timestamp"]
    CLOSE_POLICY_FIELD_NUMBER: _ClassVar[int]
    EXTRA_FIELD_NUMBER: _ClassVar[int]
    FOR_ID_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LIFETIME_FIELD_NUMBER: _ClassVar[int]
    ORDER_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    close_policy: ClosePolicy
    extra: str
    for_id: str
    id: str
    lifetime: int
    order_type: OrderType
    price: float
    quantity: float
    side: Side
    symbol: Symbol
    timestamp: int
    def __init__(self, id: _Optional[str] = ..., symbol: _Optional[_Union[Symbol, _Mapping]] = ..., side: _Optional[_Union[Side, str]] = ..., price: _Optional[float] = ..., quantity: _Optional[float] = ..., order_type: _Optional[_Union[OrderType, str]] = ..., lifetime: _Optional[int] = ..., timestamp: _Optional[int] = ..., close_policy: _Optional[_Union[ClosePolicy, str]] = ..., for_id: _Optional[str] = ..., extra: _Optional[str] = ...) -> None: ...

class Orders(_message.Message):
    __slots__ = ["orders"]
    ORDERS_FIELD_NUMBER: _ClassVar[int]
    orders: _containers.RepeatedCompositeFieldContainer[Order]
    def __init__(self, orders: _Optional[_Iterable[_Union[Order, _Mapping]]] = ...) -> None: ...

class Position(_message.Message):
    __slots__ = ["avg_price", "extra", "id", "qty", "quote_qty", "side", "symbol"]
    AVG_PRICE_FIELD_NUMBER: _ClassVar[int]
    EXTRA_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    QTY_FIELD_NUMBER: _ClassVar[int]
    QUOTE_QTY_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    avg_price: float
    extra: str
    id: int
    qty: float
    quote_qty: float
    side: Side
    symbol: Symbol
    def __init__(self, id: _Optional[int] = ..., symbol: _Optional[_Union[Symbol, _Mapping]] = ..., side: _Optional[_Union[Side, str]] = ..., qty: _Optional[float] = ..., quote_qty: _Optional[float] = ..., avg_price: _Optional[float] = ..., extra: _Optional[str] = ...) -> None: ...

class Signal(_message.Message):
    __slots__ = ["probability", "side", "source", "symbol"]
    PROBABILITY_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    probability: float
    side: Side
    source: SignalSource
    symbol: Symbol
    def __init__(self, source: _Optional[_Union[SignalSource, str]] = ..., side: _Optional[_Union[Side, str]] = ..., symbol: _Optional[_Union[Symbol, _Mapping]] = ..., probability: _Optional[float] = ...) -> None: ...

class Spread(_message.Message):
    __slots__ = ["spread", "symbol", "time"]
    SPREAD_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    spread: float
    symbol: Symbol
    time: int
    def __init__(self, symbol: _Optional[_Union[Symbol, _Mapping]] = ..., spread: _Optional[float] = ..., time: _Optional[int] = ...) -> None: ...

class Symbol(_message.Message):
    __slots__ = ["base_asset_precision", "exchange", "quote_asset_precision", "symbol"]
    BASE_ASSET_PRECISION_FIELD_NUMBER: _ClassVar[int]
    EXCHANGE_FIELD_NUMBER: _ClassVar[int]
    QUOTE_ASSET_PRECISION_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    base_asset_precision: int
    exchange: ExchangeId
    quote_asset_precision: int
    symbol: str
    def __init__(self, symbol: _Optional[str] = ..., exchange: _Optional[_Union[ExchangeId, str]] = ..., base_asset_precision: _Optional[int] = ..., quote_asset_precision: _Optional[int] = ...) -> None: ...

class SignalSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Side(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class ExchangeId(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class OrderType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class ClosePolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
