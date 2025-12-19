# sorj.py
import os
import sys
import hashlib
import argparse

# Розмір шматка: 100 МБ для прикладу. (10 * 1024 * 1024)
KiBYTE = 1024
MiBYTE = KiBYTE ** 2

KBYTE = 1000
MBYTE = KBYTE ** 2

#CHUNK_SIZE = 10 * MiBYTE

DEFAULT_BUFFER_SIZE = 4 * MiBYTE
BUFFER_STEP_SIZE = 1.5
MAX_BUFFER_SIZE = 64 * MiBYTE

DEFAULT_PLACE = "R:\\"
DEFAULT_EXT = ".CPARTA"
# --- НАЛАШТУВАННЯ ---

#CURRENT_DIR = os.getcwd()
CURRENT_PATH = DEFAULT_PLACE
CHUNK_SIZE = 500 * MiBYTE
# Константи
#BUFFER_SIZE = DEFAULT_BUFFER_SIZE
BUFFER_SIZE = MAX_BUFFER_SIZE

# ----------------------------------------------------------------

def split_file_stream(input_file_path, temp_file_path, chunk_size):
    """
    Розбиває великий файл на шматки, використовуючи один і той самий
    тимчасовий файл, який перезаписується, з ручною паузою.
    """
    if not os.path.exists(CHUNKS_DIR):
        os.makedirs(CHUNKS_DIR)
        
    chunk_size = CHUNK_SIZE
    print(f"--- РЕЖИМ РОЗБИТТЯ (Split) ---")
    print(f"Вхідний файл: {input_file_path}")
    print(f"Розмір шматка: {chunk_size / MiBYTE:.2f} MiB")
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

# ----------------------------------------------------------------

def split_file_optimized(source_path, target_ram_disk_path, chunk_limit_mb=2000):
    
    temp_filename = os.path.basename(source_path) + DEFAULT_EXT
    temp_file_path = os.path.join(target_ram_disk_path, temp_filename)
    #manifest_path = source_path + ".manifest.txt"

    if not os.path.exists(source_path):
        print(f"Помилка: Файл не знайдено.")
        return

    total_size = os.path.getsize(source_path)
    part_num = 1
    
    # === ОПТИМІЗАЦІЯ 1: Створення "вічного" буфера ===
    # Ми виділяємо 64 Мб пам'яті лише ОДИН РАЗ.
    # buffer - це як постійне відро, яке ми не викидаємо, а просто миємо.
    buffer = bytearray(BUFFER_SIZE)
    # Створюємо memoryview для швидкого доступу без копіювання
    buffer_view = memoryview(buffer)

    # Очистка маніфесту
    #with open(manifest_path, 'w', encoding='utf-8') as m:
    #    m.write(f"File: {os.path.basename(source_path)}\nSize: {total_size}\n{'-'*20}\n")

    print(f"--- РЕЖИМ SUPER-OPTIMIZED (readinto) ---")
    
    try:
        with open(source_path, 'rb') as source:
            bytes_processed_global = 0
            
            while bytes_processed_global < total_size:
                print(f"\n>>> Частина #{part_num}...", end='')
                chunk_hasher = hashlib.sha256()
                chunk_written = 0
                
                with open(temp_file_path, 'wb') as target:
                    while chunk_written < CHUNK_SIZE:
                        # Скільки треба прочитати?
                        left_in_chunk = CHUNK_SIZE - chunk_written
                        left_in_file = total_size - bytes_processed_global
                        to_read = min(BUFFER_SIZE, left_in_chunk, left_in_file)
                        
                        if to_read == 0:
                            break
                        
                        # === ОПТИМІЗАЦІЯ 2: readinto ===
                        # Ми читаємо прямо в наш існуючий buffer. 
                        # n - кількість реально прочитаних байтів (може бути менше буфера в кінці файлу)
                        n = source.readinto(buffer_view[:to_read])
                        
                        if n == 0: 
                            break # EOF (хоча ми перевіряли to_read, але про всяк випадок)

                        # Тепер у нас в buffer[0:n] лежать свіжі дані.
                        # Ми використовуємо slice (зріз) memoryview. 
                        # Це НЕ КОПІЮЄ дані, це просто вказівник "дивись звідси сюди".
                        active_data = buffer_view[:n]
                        
                        # === "РОЗДВОЄННЯ" ПОТОКУ ===
                        # Оскільки active_data - це лише посилання на пам'ять, це дуже швидко.
                        
                        # 1. Годуємо хешер
                        chunk_hasher.update(active_data)
                        
                        # 2. Пишемо на диск
                        target.write(active_data)
                        
                        chunk_written += n
                        bytes_processed_global += n

                hex_digest = chunk_hasher.hexdigest()
                #print(f"\r   [ГОТОВО] Розмір: {chunk_written/1024**2:.2f} MB | SHA256: {hex_digest[:10]}...")
                print(f"\r+ Частина #{part_num} + [{temp_filename}] Розмір: {chunk_written/MiBYTE:.2f} MB ¦ SHA256: {hex_digest}; Next?", end='')
                
                #with open(manifest_path, 'a', encoding='utf-8') as m:
                #    m.write(f"Part{part_num:03d} | {hex_digest}\n")

                #print(f"!! ДІЯ: Завантаж {temp_filename}. Потім натисни ENTER.")
                action = None
                action = input()
                if action:
                    print("Exit!")
                    sys.exit(0)
                print(f"\033M\r+ Частина #{part_num} + [{temp_filename}] Розмір: {chunk_written/MiBYTE:.2f} MB ¦ SHA256: {hex_digest};      ", end='')
                
                part_num += 1

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if os.path.exists(temp_file_path):
            try: os.remove(temp_file_path)
            except: pass
            
# ----------------------------------------------------------------

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


def validate_path(path):
    """Спеціальна функція для миттєвої перевірки шляхів."""
    if os.path.isfile(path):
        return {'type': 'file', 'path': path}
    elif os.path.isdir(path):
        return {'type': 'dir', 'path': path}
    elif path.isdigit():
        return {'type': 'num', 'value': path}
    else:
        # Якщо не файл і не папка, повертаємо як текст
        return {'type': 'text', 'path': path}

# ----------------- ВИКОНАННЯ -----------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Розумний обробник аргументів"
    )
    # Додаємо позиційні аргументи (вони обов'язкові за порядком)
    parser.add_argument("first", help="Файл, папка або текст")
    parser.add_argument("second", nargs="?", help="Папка призначення (опціонально)")
    parser.add_argument("third", nargs="?", type=int, help="Число/Розмір (опціонально)")
    #parser.add_argument("size", type=int, help="Розмір шматка у байтах")
    
    args = parser.parse_args()
    
    first_info = validate_path(args.first)
    second_info = validate_path(args.second)
    
    if first_info['type'] == 'file':
        input_file = first_info['path']
    else:
        input_file = input("Файл для розбиття: ")
    
    if not os.path.exists(input_file):
        print(f"Помилка: Файл '{input_file}' не знайдено. Завершення.")
        sys.exit(1)
    
    destination = DEFAULT_PLACE
    if second_info['type'] == 'dir':
        destination = second_info['path']
    elif second_info['type'] == 'num' :
        if int(second_info['value']) < 10000:
            CHUNK_SIZE = int(second_info['value']) * MiBYTE
        
    if not os.path.exists(destination):
        print(f"Помилка: Файл '{destination}' не знайдено. Завершення.")
        sys.exit(1)
        
    # Виконуємо розбиття
    #split_file_stream(input_file, TEMP_CHUNK_FILENAME, CHUNK_SIZE)
    split_file_optimized(input_file, destination)
    
    # Виконуємо склеювання
    # Примітка: для склеювання потрібні файли, що були створені у CHUNKS_DIR.
    # Вкажіть бажане ім'я відновленого файлу
    #JOINED_FILE = "restored_" + os.path.basename(input_file)
    
    #print("\n-------------------------------------------")
    #print(f"Для склеювання потрібні шматки в папці '{CHUNKS_DIR}'.")
    #join_file_stream(JOINED_FILE)