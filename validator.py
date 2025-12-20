import os
import hashlib
import sys

# Налаштування буфера (як у оригіналі, наприклад 64MB або 1MB)
# 1 MiB = 1024 * 1024 байт. Можна збільшити до 64 * 1024 * 1024, якщо RAM дозволяє.
BUFFER_SIZE = 64 * 1024 * 1024  # 4 MB buffer

def calculate_folder_hash(folder_path):
    """
    Рахує SHA256 для всіх файлів у папці так, ніби вони є одним склеєним файлом.
    Порядок файлів визначається їх назвою (алфавітний порядок).
    """
    
    if not os.path.exists(folder_path):
        print(f"Помилка: Папку '{folder_path}' не знайдено.")
        return

    # Отримуємо список файлів і сортуємо їх
    # ВАЖЛИВО: Файли мають бути названі так, щоб сортування було правильним 
    # (наприклад: part.001, part.002, а не part.1, part.10, part.2)
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    files.sort()

    if not files:
        print("Папка порожня.")
        return

    print(f"--- РЕЖИМ SUPER-OPTIMIZED (readinto) ---")
    print(f"Обробка папки: {folder_path}")
    print(f"Знайдено файлів: {len(files)}")
    print("-" * 40)

    # === ОПТИМІЗАЦІЯ 1: Створення "вічного" буфера ===
    buffer = bytearray(BUFFER_SIZE)
    buffer_view = memoryview(buffer)
    
    # Ініціалізуємо хешер ОДИН раз для всієї послідовності
    global_hasher = hashlib.sha256()
    
    total_bytes_processed = 0

    try:
        for idx, filename in enumerate(files):
            filepath = os.path.join(folder_path, filename)
            file_size = os.path.getsize(filepath)
            
            print(f"[{idx+1}/{len(files)}] Читання: {filename} ({file_size/1024/1024:.2f} MB)... ", end='', flush=True)

            with open(filepath, 'rb') as source:
                while True:
                    # === ОПТИМІЗАЦІЯ 2: readinto ===
                    # Читаємо прямо в буфер, отримуємо кількість прочитаних байт (n)
                    n = source.readinto(buffer_view)
                    
                    if n == 0:
                        break # Кінець поточного файлу
                    
                    # Використовуємо зріз (slice) memoryview. 
                    # Це не копіює дані, а передає вказівник на заповнену частину буфера хешеру.
                    global_hasher.update(buffer_view[:n])
                    
                    total_bytes_processed += n
            
            print("OK")

        # Фінальний розрахунок
        final_hash = global_hasher.hexdigest()
        
        print("=" * 40)
        print(f"Всього оброблено: {total_bytes_processed / (1024*1024):.2f} MB")
        print(f"ЗАГАЛЬНИЙ SHA256: {final_hash}")
        print("=" * 40)

    except Exception as e:
        print(f"\nПомилка під час обробки: {e}")
    except KeyboardInterrupt:
        print("\nПерервано користувачем.")

# Приклад використання
if __name__ == "__main__":
    # Можна передати шлях як аргумент або вписати вручну
    if len(sys.argv) > 1:
        target_dir = sys.argv[1]
    else:
        target_dir = input("Введіть шлях до папки з частинами файлу: ").strip('"')
    
    calculate_folder_hash(target_dir)
