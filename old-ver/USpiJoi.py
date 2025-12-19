import os
import sys

# --- НАЛАШТУВАННЯ ---

# Розмір шматка: 100 МБ для прикладу. (100 * 1024 * 1024)
# Змініть це значення на ваш ліміт (наприклад, 2000 * 1024 * 1024 для 2 ГБ) # 2097152000
CHUNK_SIZE = 100 * 1024 * 1024  
# Ім'я файлу для тимчасового зберігання поточного шматка (перезаписується)
TEMP_CHUNK_FILENAME = "current_chunk_for_upload.bin"
# Папка для імітації сховища та зберігання готових шматків
CHUNKS_DIR = "upload_chunks"

# --------------------

def split_file_stream(input_file_path, temp_file_path, chunk_size):
    """
    Розбиває великий файл на шматки, використовуючи один і той самий
    тимчасовий файл, який перезаписується, з ручною паузою.
    """
    if not os.path.exists(CHUNKS_DIR):
        os.makedirs(CHUNKS_DIR)
        
    print(f"--- РЕЖИМ РОЗБИТТЯ (Split) ---")
    print(f"Вхідний файл: {input_file_path}")
    print(f"Розмір шматка: {chunk_size / (1024*1024):.2f} MB")
    print(f"Тимчасовий файл: {temp_file_path}")
    print("-" * 30)

    try:
        # Відкриваємо вхідний файл для читання
        with open(input_file_path, 'rb') as f_in:
            part_number = 1
            while True:
                # 1. Читання в RAM-буфер
                data = f_in.read(chunk_size)
                if not data:
                    break  # Кінець файлу

                print(f"[{part_number}] Зчитано {len(data) / (1024*1024):.2f} MB у RAM-буфер.")

                #input("   $$$ Натисніть ENTER для продовження (перед вивантаженням шматка) $$$")

                # 2. Запис у єдиний тимчасовий файл (перезапис)
                # 'wb' - write binary (завжди створює новий файл / перезаписує існуючий)
                with open(temp_file_path, 'wb') as f_temp:
                    f_temp.write(data)
                
                print(f"   --> Тимчасовий файл: '{temp_file_path}' створено/перезаписано.")
                
                # --- ІМІТАЦІЯ ВИВАНТАЖЕННЯ ---
                # Тут ви вивантажуєте 'temp_file_path' у сховище.
                # Для демонстрації ми перейменовуємо файл на 'target_chunk_name'
                target_chunk_name = f"{os.path.basename(input_file_path)}.part.{part_number:03d}"
                temp_path_ready = os.path.join(CHUNKS_DIR, target_chunk_name)
                
                # Перейменовуємо тимчасовий файл, щоб "звільнити" його для наступного перезапису
                os.rename(temp_file_path, temp_path_ready)
                print(f"   --> ГОТОВО: Шматок перейменовано на {temp_path_ready}.")
                data = ""
                
                # 3. Ручна пауза
                input("   *** Натисніть ENTER для продовження (після вивантаження шматка) ***")
                
                # Повертаємо назву, щоб наступний шматок знову перезаписав "Temp_Current_Part.bin"
                os.rename(temp_path_ready, temp_file_path) 

                part_number += 1
        
        # Фінальне очищення
        if os.path.exists(temp_file_path):
             os.remove(temp_file_path)
        print("\n--- РОЗБИТТЯ ЗАВЕРШЕНО ---")

    except FileNotFoundError:
        print(f"ПОМИЛКА: Файл не знайдено за шляхом: {input_file_path}")
    except Exception as e:
        print(f"Непередбачена помилка: {e}")


def join_file_stream(output_file_path):
    """
    Імітує склеювання, послідовно дописуючи дані з шматків
    у кінцевий файл без його перезапису.
    """
    if not os.path.exists(CHUNKS_DIR):
        print(f"ПОМИЛКА: Не знайдено папку зі шматками '{CHUNKS_DIR}'.")
        return

    # Сортуємо файли за номером частини для правильної послідовності
    chunk_files = sorted([f for f in os.listdir(CHUNKS_DIR) if f.endswith('.part.001')[:-4].startswith('test_file')], key=lambda x: int(x.split('.')[-1]))
    if not chunk_files:
        print("Не знайдено шматків для склеювання.")
        return

    # Перевірка: якщо кінцевий файл існує, ми ДОПИСУЄМО
    if os.path.exists(output_file_path):
        print(f"УВАГА: Файл '{output_file_path}' існує. Нові дані будуть ДОПИСАНІ.")
        
    print(f"\n--- РЕЖИМ СКЛЕЮВАННЯ (Join) ---")
    print(f"Кінцевий файл: {output_file_path}")
    print("-" * 30)
    
    try:
        # 'ab' - append binary (режим дописування)
        with open(output_file_path, 'ab') as f_out:
            for part_name in chunk_files:
                part_path = os.path.join(CHUNKS_DIR, part_name)
                
                # 1. Завантаження шматка в RAM-буфер
                with open(part_path, 'rb') as f_in_part:
                    data = f_in_part.read()
                    
                print(f"Зчитано {part_name} ({len(data) / (1024*1024):.2f} MB).")
                
                # 2. Дописування даних у кінцевий файл
                f_out.write(data)
                
                # 3. Ручна пауза
                input("   *** Натисніть ENTER для продовження (після обробки шматка) ***")

        print("\n--- СКЛЕЮВАННЯ ЗАВЕРШЕНО ---")

    except Exception as e:
        print(f"Непередбачена помилка при склеюванні: {e}")

# ----------------- ВИКОНАННЯ -----------------

if __name__ == "__main__":
    
    print("Будь ласка, вкажіть шлях до файлу для розбиття:")
    input_file = input("Шлях до файлу: ")
    
    if not os.path.exists(input_file):
        print(f"Помилка: Файл '{input_file}' не знайдено. Завершення.")
        sys.exit(1)

    # Виконуємо розбиття
    split_file_stream(input_file, TEMP_CHUNK_FILENAME, CHUNK_SIZE)
    
    # Виконуємо склеювання
    # Примітка: для склеювання потрібні файли, що були створені у CHUNKS_DIR.
    # Вкажіть бажане ім'я відновленого файлу
    JOINED_FILE = "restored_" + os.path.basename(input_file)
    print("\n-------------------------------------------")
    print(f"Для склеювання потрібні шматки в папці '{CHUNKS_DIR}'.")
    join_file_stream(JOINED_FILE)