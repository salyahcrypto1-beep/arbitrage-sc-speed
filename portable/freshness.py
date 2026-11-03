from datetime import datetime

def book_is_fresh(book, now=None, max_age=30):
    try:
        now = now or datetime.now()
        age = (now - datetime.fromisoformat(book['lastUpdate'])).total_seconds()
        return 0 <= age <= max_age
    except (KeyError, ValueError, TypeError):
        return False

def fresh_books(books):
    return {symbol: book for symbol, book in books.items() if book_is_fresh(book)}
