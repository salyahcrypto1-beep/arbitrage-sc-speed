# Публичные данные и честные ограничения

В стандартном режиме нет подписанных запросов к биржевым аккаунтам, торговли, запросов баланса или вывода средств. Ключ нужен только Telegram.

| Биржа | Рынки / стаканы | Сети перевода в локальной сборке |
|---|---|---|
| KuCoin | Public REST + WebSocket | Публичные цепочки, статусы, фиксированная комиссия при наличии |
| Bitget | Public REST + WebSocket | Публичные цепочки, статусы, комиссия и контракт при наличии |
| CoinEx | Собственный публичный WebSocket + REST справочник | Публичные конфигурации ввода/вывода и комиссии |
| HTX | Public REST + WebSocket | Публичный справочник; комиссия только при явно фиксированном типе |
| Gate | Public REST + WebSocket | Публичные статусы цепочек; комиссия неизвестна, направление вывода исключается из арбитража |
| Bybit | Public REST + WebSocket | Не используется приватный wallet endpoint; метаданные сетей неизвестны |
| MEXC | Public REST + WebSocket | Не используется приватный capital endpoint; метаданные сетей неизвестны |
| BingX | Public REST + WebSocket | Не используется приватный capital endpoint; метаданные сетей неизвестны |
| OKX | Public REST + WebSocket | Не используется приватный asset endpoint; исходный арбитражный фильтр исключает OKX |
| BitMart | Собственный публичный WebSocket + REST справочник | Не подменяем отсутствие chain-level комиссии; метаданные сетей неизвестны |

Публичность цен не означает публичность всех сетевых комиссий. Поэтому охват **переводного арбитража** без ключей уже, чем охват стаканов и наблюдений маркет-мейкеров. В MM интерфейсе часть межбиржевых расчётов также требует проверенного маршрута. Неизвестные данные не превращаются в `deposit=True`, `withdraw=True` или нулевую комиссию. Неизвестный контракт остаётся неизвестным; совпадение тикера и названия сети само по себе не доказывает идентичность актива. Если оба контракта известны и различаются, арбитражный маршрут отбрасывается.

Справочник обновляется раз в 300 секунд. При ошибке обновления старые сетевые сведения данной биржи очищаются, чтобы не продолжать показывать разрешённый маршрут на устаревших данных. Стаканы публикуются нативными модулями, обычно раз в 2 секунды с TTL 10 секунд. Возраст отдельной книги проверяется независимо (30 секунд). Полные снимки и delta-потоки обрабатываются по правилам конкретной биржи.

## Официальные источники

- [CoinEx: v2 WebSocket depth](https://docs.coinex.com/api/v2/spot/market/ws/market-depth)

- [Gate: native public WebSocket](https://www.gate.com/docs/developers/apiv4/ws/en/)
- [OKX: public WebSocket and books5](https://www.okx.com/docs-v5/en/)
- [Bitget: public coins](https://www.bitget.com/legacy-docs/classic/spot/market/Get-Coin-List)
- [KuCoin: currencies](https://www.kucoin.com/en-au/docs-new/rest/spot-trading/market-data/get-all-currencies)
- [CoinEx: public deposit/withdraw configuration](https://docs.coinex.com/api/v2/assets/deposit-withdrawal/http/list-all-deposit-withdrawal-config)
- [CoinEx: market depth](https://docs.coinex.com/api/v2/spot/market/http/list-market-depth)
- [HTX: currencies and chains](https://huobiapi.github.io/docs/spot/v1/en/#apiv2-currency-amp-chains)
- [BitMart: public market API](https://developer-pro.bitmart.com/en/spot/)

Документация площадок и доступность по регионам меняются. Диагностика проверяет реальный доступ из контейнера пользователя, а не только наличие API в документации.
