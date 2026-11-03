import os
import asyncio
import logging
import ccxt.async_support as ccxt
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums.parse_mode import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from pymongo import MongoClient
from pymemcache.client import base

# Настройки
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
MONGO_URI = "mongodb://mongo:27017"

# Настраиваем бота и БД
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
client = MongoClient(MONGO_URI)
db = client.crypto_hub
users_collection = db.users
cache = base.Client(('memcached', 11211))

logging.basicConfig(level=logging.INFO)

# FSM (Finite State Machine) для последовательных шагов
class TransferState(StatesGroup):
    select_exchange_from = State()
    select_exchange_to = State()
    select_token = State()
    select_network = State()
    enter_amount = State()
    confirm_transfer = State()

class TradeState(StatesGroup):
    select_exchange = State()  # Выбор биржи
    select_token = State()  # Выбор токена
    view_order_book = State()  # Просмотр стакана
    enter_amount = State()  # Ввод суммы покупки/продажи
    confirm_trade = State()  # Подтверждение сделки

@dp.callback_query(F.data == "trade_tokens")
async def trade_tokens(callback: types.CallbackQuery, state: FSMContext):
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="BingX", callback_data="trade_bingx")],
        [types.InlineKeyboardButton(text="MEXC", callback_data="trade_mexc")]
    ])
    await callback.message.answer("Выберите биржу для торговли:", reply_markup=keyboard)
    await state.set_state(TradeState.select_exchange)

async def get_spot_markets(exchange_name, api_key, api_secret):
    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        })
        await exchange.load_markets()
        await exchange.close()

        return [market for market in exchange.symbols if "/" in market]  # Оставляем только спотовые пары
    except Exception as e:
        logging.error(f"Ошибка загрузки рынков {exchange_name}: {e}")
        return []

@dp.callback_query(lambda c: c.data.startswith("trade_"))
async def select_token(callback: types.CallbackQuery, state: FSMContext):
    exchange = callback.data.split("_")[1]
    user = users_collection.find_one({"user_id": callback.message.chat.id})
    api = user[exchange]

    tokens = await get_spot_markets(exchange, api["api_key"], api["api_secret"])

    if not tokens:
        await callback.message.answer("❌ Ошибка загрузки токенов. Попробуйте позже.")
        return

    await state.update_data(exchange=exchange, tokens=tokens, page=0)
    await show_token_list(callback.message, state)

async def show_token_list(message, state, edit=False):
    data = await state.get_data()
    tokens = data["tokens"]
    page = data["page"]

    start = page * 10
    end = start + 10
    tokens_on_page = tokens[start:end]

    keyboard = [[types.InlineKeyboardButton(text=token, callback_data=f"view_orderbook_{token}")]
                 for token in tokens_on_page]

    navigation_buttons = []
    if page > 0:
        navigation_buttons.append(types.InlineKeyboardButton(text="⬅️ Назад", callback_data="prev_page"))
    if end < len(tokens):
        navigation_buttons.append(types.InlineKeyboardButton(text="➡️ Вперед", callback_data="next_page"))

    if navigation_buttons:
        keyboard.append(navigation_buttons)

    if edit:
        await message.edit_text("Выберите токен:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard))
    else:
        await message.answer("Выберите токен:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard))

@dp.callback_query(lambda c: c.data in ["prev_page", "next_page"])
async def change_page(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    page = data["page"]

    if callback.data == "prev_page":
        page -= 1
    else:
        page += 1

    await state.update_data(page=page)
    await show_token_list(callback.message, state, edit=True)


@dp.callback_query(lambda c: c.data in ["prev_page", "next_page"])
async def change_page(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    page = data["page"]

    if callback.data == "prev_page":
        page -= 1
    else:
        page += 1

    await state.update_data(page=page)
    await show_token_list(callback.message, state)

async def get_order_book(exchange_name, api_key, api_secret, token):
    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        })
        await exchange.load_markets()

        # Логируем для отладки
        print(f"🔍 Поиск токена на {exchange_name}: {token}")
        
        if exchange_name == "mexc":
            possible_formats = [
                token,                          # 10000COQ/USDT:USDT
                token.split(":")[0],            # 10000COQ/USDT (убираем :USDT)
                token.replace("/", "_"),        # 10000COQ_USDT
                token.replace("/", ""),         # 10000COQUSDT
                token.replace("/", "-")         # 10000COQ-USDT
            ]

            # Найти первое совпадение
            token = next((t for t in possible_formats if t in exchange.symbols), None)

        if not token or token not in exchange.symbols:
            logging.error(f"❌ Символ {token} не найден на бирже {exchange_name}")
            await exchange.close()
            return None

        # Проверяем, поддерживает ли биржа стакан
        if not exchange.has.get("fetchOrderBook", False):
            logging.error(f"❌ {exchange_name} не поддерживает fetch_order_book()")
            await exchange.close()
            return None

        # Запрашиваем стакан
        order_book = await exchange.fetch_order_book(token)
        await exchange.close()

        return order_book
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки стакана {exchange_name}: {e}")
        await exchange.close()
        return None



@dp.callback_query(lambda c: c.data.startswith("view_orderbook_"))
async def view_order_book(callback: types.CallbackQuery, state: FSMContext):
    try:
        _, token = callback.data.split("view_orderbook_", 1)  # Теперь правильно парсим токен

        if not token:
            await callback.message.answer("❌ Ошибка! Некорректное название токена.")
            return

        data = await state.get_data()
        exchange = data.get("exchange")

        if not exchange:
            await callback.message.answer("❌ Ошибка! Биржа не выбрана.")
            return

        user = users_collection.find_one({"user_id": callback.message.chat.id})
        api = user.get(exchange)

        if not api:
            await callback.message.answer("❌ Ошибка! Не найдены API-ключи для биржи.")
            return

        # Получаем стакан
        order_book = await get_order_book(exchange, api["api_key"], api["api_secret"], token)

        if not order_book:
            await callback.message.answer(f"❌ Ошибка загрузки стакана {token} на {exchange}. Попробуйте другой токен.")
            return

        bids = order_book["bids"][:15]  # Покупатели
        asks = order_book["asks"][:15]  # Продавцы

        if not bids or not asks:
            await callback.message.answer(f"❌ Нет данных о стакане {token} на {exchange}.")
            return

        text = f"📊 **Стакан {token} на {exchange}**\n\n"
        text += "🔹 Покупатели:\n"
        text += "\n".join([f"{p[0]}$ - {p[1]}" for p in bids])
        text += "\n\n🔸 Продавцы:\n"
        text += "\n".join([f"{p[0]}$ - {p[1]}" for p in asks])

        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🟢 Купить", callback_data=f"buy_{token}")],
            [types.InlineKeyboardButton(text="🔴 Продать", callback_data=f"sell_{token}")]
        ])

        await state.update_data(token=token)
        await callback.message.answer(text, reply_markup=keyboard)

    except Exception as e:
        logging.error(f"❌ Ошибка в обработке view_order_book: {e}")
        await callback.message.answer("❌ Произошла ошибка. Попробуйте снова.")


@dp.callback_query(lambda c: c.data == "transfer_and_trade")
async def transfer_funds(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    exchange_obj = data["exchange_obj"]
    usdt_amount = data["usdt_amount"]

    try:
        # ✅ Выполняем перевод USDT с Funding на Spot
        transfer = await exchange_obj.transfer("USDT", usdt_amount, "funding", "spot")

        if transfer:
            await callback.message.answer(f"🔄 Перевод **{usdt_amount} USDT** на спотовый счёт выполнен!\n")
            await execute_trade(callback, state)  # Повторяем попытку покупки
        else:
            await callback.message.answer("❌ Ошибка при переводе USDT. Попробуйте вручную.")

    except Exception as e:
        logging.error(f"Ошибка перевода USDT: {e}")
        await callback.message.answer("❌ Ошибка при переводе. Проверьте баланс и попробуйте снова.")

    await state.clear()


@dp.callback_query(lambda c: c.data.startswith(("buy_", "sell_")))
async def enter_trade_amount(callback: types.CallbackQuery, state: FSMContext):
    action, token = callback.data.split("_")
    await state.update_data(action=action, token=token)

    await callback.message.answer(f"Введите сумму для {'покупки' if action == 'buy' else 'продажи'} {token}:")
    await state.set_state(TradeState.enter_amount)


@dp.message(TradeState.enter_amount)
async def confirm_trade(message: types.Message, state: FSMContext):
    data = await state.get_data()
    action = data["action"]
    token = data["token"]
    exchange = data["exchange"]
    
    try:
        usdt_amount = float(message.text)
        if usdt_amount <= 0:
            raise ValueError("Сумма должна быть больше нуля.")
    except ValueError:
        await message.answer("❌ Введите корректную сумму в USDT.")
        return

    # Получаем стакан (глубину рынка)
    user = users_collection.find_one({"user_id": message.chat.id})
    api = user.get(exchange)
    
    order_book = await get_order_book(exchange, api["api_key"], api["api_secret"], token)
    
    if not order_book or not order_book["asks"]:
        await message.answer("❌ Ошибка получения рыночной цены. Попробуйте позже.")
        return

    # Берем лучшую (минимальную) цену из стакана продаж
    best_price = order_book["asks"][0][0]  # Цена первого продавца
    
    token_amount = usdt_amount / best_price  # Считаем, сколько токенов можно купить
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_trade")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_trade")]
    ])

    await state.update_data(usdt_amount=usdt_amount, token_amount=token_amount)
    await message.answer(
        f"Вы покупаете **{token}** на **{usdt_amount} USDT**\n"
        f"Текущая цена: **{best_price} USDT**\n"
        f"Вы получите ~ **{token_amount:.6f} {token}**\n"
        f"Подтвердите сделку.",
        reply_markup=keyboard
    )
    await state.set_state(TradeState.confirm_trade)

@dp.callback_query(lambda c: c.data == "confirm_trade")
async def execute_trade(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    exchange_name = data["exchange"]
    token = data["token"]
    usdt_amount = data["usdt_amount"]
    token_amount = data["token_amount"]

    user = users_collection.find_one({"user_id": callback.message.chat.id})
    api = user.get(exchange_name)

    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api["api_key"],
            'secret': api["api_secret"],
            'enableRateLimit': True,
        })

        # ✅ Загружаем рынки
        await exchange.load_markets()

        # ✅ Отправляем рыночный ордер
        order = await exchange.create_market_buy_order(token, token_amount)

        logging.info(f"🔍 Ордер создан: {order}")
        print(order)

        order_id = order.get("id")
        order_status = order.get("status")

        # ✅ Если ордер не сразу закрыт, ждём его завершения
        if order_status != "closed":
            await callback.message.answer("⏳ Ордер создан. Ожидаем его исполнения...")

            for _ in range(15):  # Ожидаем до 30 секунд (15 * 2 сек)
                await asyncio.sleep(2)
                order_info = await exchange.fetch_order(order_id, token)
                order_status = order_info.get("status")

                logging.info(f"🔍 Проверяем статус ордера: {order_status}")

                if order_status == "closed":
                    break

        # ✅ Проверяем, исполнился ли ордер
        if order_status == "closed":
            await callback.message.answer(
                f"✅ Покупка **{token_amount:.6f} {token}** на **{usdt_amount} USDT** успешно выполнена!\n"
                f"📄 Order ID: `{order_id}`\n"
                f"🔄 Проверяем баланс..."
            )

            # ✅ Ждём 5 секунд, чтобы биржа обновила баланс
            await asyncio.sleep(5)

            # ✅ Получаем баланс токена после сделки
            balance = await exchange.fetch_balance()

            logging.info(f"🔍 Проверяем баланс токена: {token}")
            logging.info(f"📊 Полный баланс: {balance['total']}")

            # ✅ Попробуем разные варианты названия токена
            possible_token_names = [
                token,                          # Полное имя COQ/USDT
                token.split("/")[0],            # Только COQ
                token.replace("/", ""),         # COQUSDT
                token.replace("/", "_"),        # COQ_USDT
            ]

            token_balance = next(
                (balance["total"].get(name, 0) for name in possible_token_names if name in balance["total"]),
                0
            )

            await exchange.close()

            if token_balance > 0:
                await callback.message.answer(f"🎉 Баланс обновлён! У вас теперь {token_balance:.6f} {token.split('/')[0]}")
            else:
                await callback.message.answer("⚠️ Сделка прошла, но баланс не обновился. Проверьте вручную.")

        else:
            await callback.message.answer("⚠️ Ордер не был исполнен за 30 секунд. Проверьте вручную.")

    except ccxt.BaseError as e:
        logging.error(f"❌ Ошибка выполнения сделки на {exchange_name}: {e}")
        await callback.message.answer(f"❌ Ошибка: {e}. Попробуйте другой токен.")

    except Exception as e:
        logging.error(f"❌ Неизвестная ошибка на {exchange_name}: {e}")
        await callback.message.answer("❌ Ошибка при выполнении сделки. Попробуйте позже.")

    await state.clear()

# ✅ Выбор сети для перевода
@dp.callback_query(lambda c: c.data.startswith("token_"))
async def select_network(callback: types.CallbackQuery, state: FSMContext):
    token = callback.data.split("_")[1]
    data = await state.get_data()
    from_exchange = data["from_exchange"]
    to_exchange = data["to_exchange"]

    user = users_collection.find_one({"user_id": callback.message.chat.id})
    api_from = user[from_exchange]
    api_to = user[to_exchange]

    # Получаем доступные сети на обеих биржах
    networks_from = await get_token_networks(from_exchange, api_from["api_key"], api_from["api_secret"], token)
    networks_to = await get_token_networks(to_exchange, api_to["api_key"], api_to["api_secret"], token)

    # Оставляем только те сети, которые есть на обеих биржах
    common_networks = {network: networks_from[network] for network in networks_from if network in networks_to}

    if not common_networks:
        await callback.message.answer("❌ Нет общих сетей между биржами! Выберите другой токен.")
        return

    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=f"{network} (Комиссия: {fee})", callback_data=f"network_{network}")]
        for network, fee in common_networks.items()
    ])

    await state.update_data(token=token)
    await callback.message.answer("Выберите сеть для перевода:", reply_markup=keyboard)
    await state.set_state(TransferState.select_network)



# ✅ Ввод суммы для перевода
@dp.callback_query(lambda c: c.data.startswith("network_"))
async def enter_amount(callback: types.CallbackQuery, state: FSMContext):
    network = callback.data.split("_")[1]
    await state.update_data(network=network)

    await callback.message.answer("Введите сумму для перевода:")
    await state.set_state(TransferState.enter_amount)

# ✅ Выбор токена для перевода
@dp.callback_query(lambda c: c.data.startswith("to_"))
async def select_token(callback: types.CallbackQuery, state: FSMContext):
    to_exchange = callback.data.split("_")[1]
    data = await state.get_data()
    from_exchange = data["from_exchange"]

    user = users_collection.find_one({"user_id": callback.message.chat.id})
    api_from = user[from_exchange]

    balance = await get_balance(from_exchange, api_from["api_key"], api_from["api_secret"])
    tokens = [token for token, amount in balance.items() if amount > 0]

    if not tokens:
        await callback.message.answer("❌ Недостаточно средств для перевода.")
        return

    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=token, callback_data=f"token_{token}")] for token in tokens
    ])
    
    await state.update_data(to_exchange=to_exchange)
    await callback.message.answer("Выберите токен для перевода:", reply_markup=keyboard)
    await state.set_state(TransferState.select_token)


# ✅ Выбор биржи получения токенов
@dp.callback_query(lambda c: c.data.startswith("from_"))
async def select_exchange_to(callback: types.CallbackQuery, state: FSMContext):
    from_exchange = callback.data.split("_")[1]
    await state.update_data(from_exchange=from_exchange)

    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="BingX", callback_data="to_bingx")],
        [types.InlineKeyboardButton(text="MEXC", callback_data="to_mexc")]
    ])
    
    await callback.message.answer("Выберите биржу, на которую хотите перевести токены:", reply_markup=keyboard)
    await state.set_state(TransferState.select_exchange_to)

# ✅ Исправленная функция получения баланса для BingX.io и других бирж
async def get_balance(exchange_name, api_key, api_secret):
    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'  # Используем только спотовый рынок для BingX.io
            }
        })

        await exchange.load_markets()  # Загружаем рынки перед запросом
        balance = await exchange.fetch_balance()

        await exchange.close()

        # Проверяем, что баланс корректно получен
        if not balance or "total" not in balance:
            raise ValueError(f"Некорректный ответ от API {exchange_name}")

        return balance["total"]
    
    except Exception as e:
        logging.error(f"Ошибка получения баланса {exchange_name}: {e}")
        return {}


# ✅ Получение списка доступных сетей и комиссий для токена
async def get_token_networks(exchange_name, api_key, api_secret, token):
    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        })

        # Получаем список доступных методов вывода средств
        currencies = await exchange.fetch_currencies()
        await exchange.close()

        if token in currencies and "networks" in currencies[token]:
            return {
                network: currencies[token]["networks"][network]["fee"]
                for network in currencies[token]["networks"]
            }
        else:
            return {}
    except Exception as e:
        logging.error(f"Ошибка получения сетей {exchange_name}: {e}")
        return {}

# ✅ Отправка перевода через API CCXT
async def send_transfer(exchange_name, api_key, api_secret, token, amount, address, network):
    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        })

        # Отправляем запрос на вывод средств
        tx = await exchange.withdraw(token, float(amount), address, {'network': network})
        await exchange.close()
        return tx
    except Exception as e:
        logging.error(f"Ошибка перевода {exchange_name}: {e}")
        return None

# ✅ Обработчик кнопки "📊 Баланс"
@dp.callback_query(lambda c: c.data == "check_balance")
async def check_balance(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()  # Очищаем состояние, чтобы бот реагировал на кнопки
    
    user = users_collection.find_one({"user_id": callback.message.chat.id})
    if not user:
        await callback.message.answer("❌ API-ключи не найдены! Введите заново.")
        return

    bingx_api = user.get("bingx")
    mexc_api = user.get("mexc")

    if not bingx_api or not mexc_api:
        await callback.message.answer("❌ API-ключи не найдены! Введите заново.")
        return

    await callback.message.answer("⏳ Получаем баланс...")

    bingx_balance = await get_balance("bingx", bingx_api["api_key"], bingx_api["api_secret"])
    mexc_balance = await get_balance("mexc", mexc_api["api_key"], mexc_api["api_secret"])

    if not isinstance(bingx_balance, dict) or not isinstance(mexc_balance, dict):
        await callback.message.answer("❌ Ошибка получения баланса! Проверьте API-ключи.")
        return

    text = "📊 <b>Ваш баланс:</b>\n\n"

    if bingx_balance:
        text += "🔹 <b>BingX:</b>\n" + "\n".join([f"{k}: {v}" for k, v in bingx_balance.items() if v > 0]) + "\n\n"
    else:
        text += "🔹 <b>BingX:</b> ❌ Нет данных\n\n"

    if mexc_balance:
        text += "🔹 <b>MEXC:</b>\n" + "\n".join([f"{k}: {v}" for k, v in mexc_balance.items() if v > 0]) + "\n\n"
    else:
        text += "🔹 <b>MEXC:</b> ❌ Нет данных\n\n"

    await callback.message.answer(text)

# ✅ Перевод: выбор сети
@dp.callback_query(lambda c: c.data.startswith("token_"))
async def select_network(callback: types.CallbackQuery, state: FSMContext):
    token = callback.data.split("_")[1]
    data = await state.get_data()
    from_exchange = data["from_exchange"]
    
    user = users_collection.find_one({"user_id": callback.message.chat.id})
    api_from = user[from_exchange]

    networks = await get_token_networks(from_exchange, api_from["api_key"], api_from["api_secret"], token)

    if not networks:
        await callback.message.answer("❌ Не удалось получить список сетей. Попробуйте позже.")
        return

    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=f"{network} (Комиссия: {fee})", callback_data=f"network_{network}")]
        for network, fee in networks.items()
    ])
    await state.update_data(token=token)
    await callback.message.answer("Выберите сеть для перевода:", reply_markup=keyboard)
    await state.set_state(TransferState.select_network)

# ✅ Исправленная функция получения баланса для BingX.io и других бирж
async def get_balance(exchange_name, api_key, api_secret):
    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'  # Используем только спотовый рынок для BingX.io
            }
        })

        await exchange.load_markets()  # Загружаем рынки перед запросом
        balance = await exchange.fetch_balance()

        await exchange.close()

        # Проверяем, что баланс корректно получен
        if not balance or "total" not in balance:
            raise ValueError(f"Некорректный ответ от API {exchange_name}")

        return balance["total"]
    
    except Exception as e:
        logging.error(f"Ошибка получения баланса {exchange_name}: {e}")
        return {}


# ✅ Исправленная функция отправки перевода через API CCXT
async def send_transfer(exchange_name, api_key, api_secret, token, amount, address, network):
    try:
        # Убираем спецсимволы из адреса (актуально для MEXC)
        clean_address = "".join(filter(str.isalnum, address))

        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        })

        # Отправляем запрос на вывод средств
        tx = await exchange.withdraw(token, float(amount), clean_address, {'network': network})
        await exchange.close()
        return tx

    except Exception as e:
        logging.error(f"Ошибка перевода {exchange_name}: {e}")
        return None


# ✅ Исправленная функция получения адреса депозита
async def get_deposit_address(exchange_name, api_key, api_secret, token, network):
    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        })

        address_info = await exchange.fetch_deposit_address(token, {'network': network})
        await exchange.close()

        if 'address' in address_info:
            return "".join(filter(str.isalnum, address_info['address']))  # Чистим адрес от спецсимволов
        else:
            return None

    except Exception as e:
        logging.error(f"Ошибка получения адреса депозита {exchange_name}: {e}")
        return None

# ✅ Подтверждение перевода
@dp.message(F.text, TransferState.enter_amount)
async def confirm_transfer(message: types.Message, state: FSMContext):
    await state.update_data(amount=message.text)

    data = await state.get_data()
    text = f"""
    ✅ Подтверждение перевода:
    - Биржа отправки: {data['from_exchange']}
    - Биржа получения: {data['to_exchange']}
    - Токен: {data['token']}
    - Сеть: {data['network']}
    - Сумма: {data['amount']}
    ❓ Подтвердить перевод?
    """
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✅ Да", callback_data="confirm_transfer")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_transfer")]
    ])
    
    await message.answer(text, reply_markup=keyboard)
    await state.set_state(TransferState.confirm_transfer)


# ✅ Завершение перевода с автоматическим выбором адреса получения
@dp.callback_query(lambda c: c.data == "confirm_transfer")
async def process_transfer(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()

    user = users_collection.find_one({"user_id": callback.message.chat.id})
    api_from = user[data["from_exchange"]]
    api_to = user[data["to_exchange"]]

    await callback.message.answer("🔄 Получаем адрес для депозита...")

    # Получаем адрес для депозита на бирже-получателе
    deposit_address = await get_deposit_address(
        data["to_exchange"],
        api_to["api_key"],
        api_to["api_secret"],
        data["token"],
        data["network"]
    )

    if not deposit_address:
        await callback.message.answer("❌ Ошибка получения адреса депозита. Проверьте поддержку сети на бирже и попробуйте снова.")
        return

    await callback.message.answer(f"📩 Адрес депозита на {data['to_exchange']}:\n`{deposit_address}`")

    await callback.message.answer("🔄 Выполняем перевод...")

    # Отправка перевода на полученный адрес
    tx = await send_transfer(
        data["from_exchange"],
        api_from["api_key"],
        api_from["api_secret"],
        data["token"],
        data["amount"],
        deposit_address,
        data["network"]
    )

    if tx:
        await callback.message.answer(f"✅ Перевод {data['amount']} {data['token']} через {data['network']} выполнен!\nTX ID: `{tx['id']}`")
    else:
        await callback.message.answer("❌ Ошибка при выполнении перевода. Проверьте данные и попробуйте снова.")

    await state.clear()


# ✅ Отмена перевода
@dp.callback_query(lambda c: c.data == "cancel_transfer")
async def cancel_transfer(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("❌ Перевод отменен.")
    await state.clear()

@dp.message(Command("start"))
async def start_command(message: types.Message):
    user = users_collection.find_one({"user_id": message.chat.id})

    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Баланс", callback_data="check_balance")],
        [types.InlineKeyboardButton(text="🔄 Перевести токен", callback_data="transfer_token")],
        [types.InlineKeyboardButton(text="🛒 Купить/Продать токены", callback_data="trade_tokens")],
        [types.InlineKeyboardButton(text="🔑 Изменить API-ключи", callback_data="update_keys")]
    ])

    if user:
        await message.answer("✅ Добро пожаловать в Crypto Hub!\nВыберите действие:", reply_markup=keyboard)
    else:
        await message.answer("👋 Введите API-ключи:\n`BINGX_API_KEY:BINGX_SECRET,MEXC_API_KEY:MEXC_SECRET`")



@dp.callback_query(F.data == "transfer_token")
async def transfer_token(callback: types.CallbackQuery, state: FSMContext):
    user = users_collection.find_one({"user_id": callback.message.chat.id})

    if not user or not user.get("bingx") or not user.get("mexc"):
        await callback.message.answer("❌ API-ключи не найдены! Введите их заново.")
        return

    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="BingX", callback_data="from_bingx")],
        [types.InlineKeyboardButton(text="MEXC", callback_data="from_mexc")]
    ])
    
    await callback.message.answer("Выберите биржу, с которой хотите перевести токены:", reply_markup=keyboard)
    await state.set_state(TransferState.select_exchange_from)

# ✅ Машина состояний для обновления API-ключей
class UpdateKeysState(StatesGroup):
    select_exchange = State()
    enter_keys = State()

# ✅ Команда для обновления API-ключей
@dp.callback_query(lambda c: c.data == "update_keys")
async def update_keys_start(callback: types.CallbackQuery, state: FSMContext):
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🔑 BingX", callback_data="update_bingx")],
        [types.InlineKeyboardButton(text="🔑 MEXC", callback_data="update_mexc")]
    ])

    await callback.message.answer("Выберите биржу, для которой хотите обновить API-ключи:", reply_markup=keyboard)
    await state.set_state(UpdateKeysState.select_exchange)

# ✅ Выбор биржи для обновления API-ключей
@dp.callback_query(lambda c: c.data.startswith("update_"))
async def select_exchange_for_update(callback: types.CallbackQuery, state: FSMContext):
    exchange = callback.data.split("_")[1]  # Получаем имя биржи (bingx или mexc)
    await state.update_data(exchange=exchange)

    await callback.message.answer(f"Введите новые API-ключи для {exchange.upper()} в формате:\n`API_KEY:SECRET_KEY`")
    await state.set_state(UpdateKeysState.enter_keys)

# ✅ Ввод новых API-ключей
@dp.message(UpdateKeysState.enter_keys)
async def update_keys_in_db(message: types.Message, state: FSMContext):
    data = await state.get_data()
    exchange = data["exchange"]

    try:
        api_key, secret_key = message.text.split(":")
    except ValueError:
        await message.answer("❌ Формат неверный! Введите ключи в формате:\n`API_KEY:SECRET_KEY`")
        return

    # Обновляем только ключи указанной биржи в MongoDB
    users_collection.update_one(
        {"user_id": message.chat.id},
        {"$set": {exchange: {"api_key": api_key, "api_secret": secret_key}}},
        upsert=True
    )

    await message.answer(f"✅ API-ключи для {exchange.upper()} обновлены!")
    await state.clear()





# ✅ Функция для регистрации всех обработчиков в диспетчере
def register_handlers(dp: Dispatcher):
    dp.message.register(start_command)
    dp.callback_query.register(check_balance, F.data == "check_balance")
    dp.callback_query.register(transfer_token, F.data == "transfer_token")
    dp.callback_query.register(select_exchange_to, F.data.startswith("from_"))
    dp.callback_query.register(select_token, F.data.startswith("to_"))
    dp.callback_query.register(select_network, F.data.startswith("token_"))
    dp.callback_query.register(enter_amount, F.data.startswith("network_"))
    dp.message.register(confirm_transfer, F.text, TransferState.enter_amount)  # ✅ Исправлено
    dp.callback_query.register(process_transfer, F.data == "confirm_transfer")
    dp.callback_query.register(cancel_transfer, F.data == "cancel_transfer")
    dp.callback_query.register(update_keys_start, F.data == "update_keys")  # ✅ Обработчик для кнопки "Изменить API-ключи"
    dp.callback_query.register(select_exchange_for_update, F.data.startswith("update_"))
    dp.message.register(update_keys_in_db, UpdateKeysState.enter_keys)




# ✅ Запуск бота
async def main():
    register_handlers(dp)  # Регистрируем обработчики
    await dp.start_polling(bot) # Запуск бота

if __name__ == "__main__":
    asyncio.run(main())
