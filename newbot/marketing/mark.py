import os
import telebot
from telebot import types
import random
from diskcache import Cache
from datetime import datetime, timedelta
import schedule
import time
import threading

# Инициализация бота и кэша
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
bot = telebot.TeleBot(BOT_TOKEN)
cache = Cache("./cache")

# Команды
WHITE_TEAM = ["Ильсур", "Раиль", "Юлия", "Регина", "Джемиль", "Александр"]
BLACK_TEAM = ["Шамиль", "Азиз", "Данил", "Денис", "Тимур", "Камиль"]

# Мотивационные сообщения
MOTIVATIONAL_MESSAGES = [
    "Отличная работа! Так держать!",
    "Ты на верном пути к успеху!",
    "Продолжай в том же духе!",
    "Ты настоящий профессионал!",
    "Твой успех вдохновляет команду!",
    "Ты сегодня звезда продаж!",
    "Невероятный результат!",
    "Ты устанавливаешь новые стандарты!",
    "Твой вклад неоценим!",
    "Ты движешься к новым высотам!"
]

def get_chat_data(chat_id):
    if not cache.get(str(chat_id)):
        cache.set(str(chat_id), {
            "white_team": {name: {"sales": 0, "amount": 0} for name in WHITE_TEAM},
            "black_team": {name: {"sales": 0, "amount": 0} for name in BLACK_TEAM},
            "last_reset": datetime.now()
        })
    return cache.get(str(chat_id))

def save_chat_data(chat_id, data):
    cache.set(str(chat_id), data)

def format_number(num):
    return f"{num:,}".replace(",", " ")

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "Добро пожаловать! Используйте /help для получения списка команд.")

@bot.message_handler(commands=['help'])
def send_help(message):
    help_text = """
    Доступные команды:
    /SaleAdd <имя> <сумма> - Добавить продажу
    /SaleRemove <имя> <сумма> - Удалить продажу
    /leaderboard - Показать турнирную таблицу
    /reset - Сбросить статистику
    """
    bot.reply_to(message, help_text)

@bot.message_handler(commands=['SaleAdd'])
def add_sale(message):
    chat_id = message.chat.id
    args = message.text.split()[1:]
    if len(args) != 2:
        bot.reply_to(message, "Неправильный формат. Используйте: /SaleAdd <имя> <сумма>")
        return

    name, amount = args[0], args[1]
    try:
        amount = int(amount)
    except ValueError:
        bot.reply_to(message, "Сумма должна быть числом.")
        return

    data = get_chat_data(chat_id)
    team = "white_team" if name in WHITE_TEAM else "black_team" if name in BLACK_TEAM else None
    if not team:
        bot.reply_to(message, f"Игрок {name} не найден в командах.")
        return

    data[team][name]["sales"] += 1
    data[team][name]["amount"] += amount
    save_chat_data(chat_id, data)

    bot.reply_to(message, f"Продажа добавлена для {name}. {random.choice(MOTIVATIONAL_MESSAGES)}")

@bot.message_handler(commands=['SaleRemove'])
def remove_sale(message):
    chat_id = message.chat.id
    args = message.text.split()[1:]
    if len(args) != 2:
        bot.reply_to(message, "Неправильный формат. Используйте: /SaleRemove <имя> <сумма>")
        return

    name, amount = args[0], args[1]
    try:
        amount = int(amount)
    except ValueError:
        bot.reply_to(message, "Сумма должна быть числом.")
        return

    data = get_chat_data(chat_id)
    team = "white_team" if name in WHITE_TEAM else "black_team" if name in BLACK_TEAM else None
    if not team:
        bot.reply_to(message, f"Игрок {name} не найден в командах.")
        return

    if data[team][name]["sales"] > 0 and data[team][name]["amount"] >= amount:
        data[team][name]["sales"] -= 1
        data[team][name]["amount"] -= amount
        save_chat_data(chat_id, data)
        bot.reply_to(message, f"Продажа удалена у {name}.")
    else:
        bot.reply_to(message, f"Невозможно удалить продажу у {name}. Недостаточно продаж или суммы.")

@bot.message_handler(commands=['leaderboard'])
def show_leaderboard(message):
    chat_id = message.chat.id
    data = get_chat_data(chat_id)

    leaderboard = "ТУРНИРНАЯ ТАБЛИЦА🏆\n\n"

    total_sales = sum(player["sales"] for team in data.values() for player in team.values())
    total_amount = sum(player["amount"] for team in data.values() for player in team.values())
    leaderboard += f"Всего - {format_number(total_amount)} ₽ - {total_sales}\n\n"

    for team_name, team_data in [("КОМАНДА БЕЛЫХ⚪️", data["white_team"]), ("КОМАНДА ЧЕРНЫХ⚫️", data["black_team"])]:
        team_sales = sum(player["sales"] for player in team_data.values())
        team_amount = sum(player["amount"] for player in team_data.values())
        leaderboard += f"{team_name}\n\n"
        leaderboard += f"Результат - {format_number(team_amount)} ₽ - {team_sales}\n\n"

        for name, stats in team_data.items():
            leaderboard += f"{name} - {format_number(stats['amount'])} ₽ - {stats['sales']}\n"
        leaderboard += "\n"

    bot.reply_to(message, leaderboard)

@bot.message_handler(commands=['reset'])
def reset_stats(message):
    chat_id = message.chat.id
    data = get_chat_data(chat_id)
    for team in data.values():
        for player in team.values():
            player["sales"] = 0
            player["amount"] = 0
    data["last_reset"] = datetime.now()
    save_chat_data(chat_id, data)
    bot.reply_to(message, "Статистика сброшена.")

def announce_winners():
    for chat_id in cache.iterkeys():
        data = get_chat_data(chat_id)
        white_team = data["white_team"]
        black_team = data["black_team"]

        white_sales = sum(player["sales"] for player in white_team.values())
        white_amount = sum(player["amount"] for player in white_team.values())
        black_sales = sum(player["sales"] for player in black_team.values())
        black_amount = sum(player["amount"] for player in black_team.values())

        winner_team = "Белые" if white_amount > black_amount else "Черные" if black_amount > white_amount else "Ничья"
        winner_sales = "Белые" if white_sales > black_sales else "Черные" if black_sales > white_sales else "Ничья"

        top_seller_name = max(white_team.items(), key=lambda x: x[1]["amount"])[0] if white_amount > black_amount else max(black_team.items(), key=lambda x: x[1]["amount"])[0] if black_amount > white_amount else "Ничья"
        top_seller_sales = max(white_team.items(), key=lambda x: x[1]["sales"])[0] if white_sales > black_sales else max(black_team.items(), key=lambda x: x[1]["sales"])[0] if black_sales > white_sales else "Ничья"

        message = f"Итоги дня:\n\n"
        message += f"Победила команда {winner_team} по сумме продаж!\n"
        message += f"Победила команда {winner_sales} по количеству продаж!\n"
        message += f"Лучший продавец по сумме продаж - {top_seller_name}!\n"
        message += f"Лучший продавец по количеству продаж - {top_seller_sales}!\n"

        bot.send_message(chat_id, message)

        # Сброс статистики после объявления победителей
        for team in [white_team, black_team]:
            for player in team.values():
                player["sales"] = 0
                player["amount"] = 0
        data["last_reset"] = datetime.now()  # <- Проблема здесь
        save_chat_data(chat_id, data)




def run_schedule():
    schedule.every().day.at("23:59").do(announce_winners)
    
    scheduler_thread = threading.Thread(target=run_scheduled_tasks)
    scheduler_thread.start()
    
    try:
        bot.polling(none_stop=True, timeout=60)
    except Exception as e:
        print(f"Ошибка в bot.polling(): {e}")
        bot.stop_polling()

def run_scheduled_tasks():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run_schedule()
