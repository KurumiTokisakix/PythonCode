import pyodbc
import html
from bs4 import BeautifulSoup
import re

# ====== NGƯỜI DÙNG NHẬP ======
# Các ký tự đặc biệt cho phép loại bỏ nếu nằm ở đầu hoặc cuối
special_chars_start = '.,\t'   # ví dụ: dấu chấm, phẩy, tab
special_chars_end = '*#@\t'    # ví dụ: sao, thăng, @, tab

# Escape các ký tự để dùng trong regex
escaped_start = re.escape(special_chars_start)
escaped_end = re.escape(special_chars_end)

# Regex pattern xóa tất cả chuỗi ký tự đặc biệt được chỉ định nếu nằm đầu/cuối
start_pattern = rf'^([{escaped_start}]+)'
end_pattern = rf'([{escaped_end}]+)$'

# ====== HÀM XỬ LÝ ======
def clean_html(text):
    if not text:
        return ''
    text = html.unescape(text)
    text = BeautifulSoup(text, "html.parser").get_text()
    return text.strip()

def remove_special_start(text):
    return re.sub(start_pattern, '', text)

def remove_special_end(text):
    return re.sub(end_pattern, '', text)

def initcap_and_convert(text, conv_dict):
    words = text.split()
    words = [word.capitalize() for word in words]
    for i, word in enumerate(words):
        if word in conv_dict:
            words[i] = conv_dict[word]
    return ' '.join(words)

# ====== KẾT NỐI VÀ XỬ LÝ DB ======
server133 = '192.168.1.39,63839'
username133 = 'BI.Data.Input'
password133 = 'dkOeknsdLkasdnu98('
conn_string133 = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server133};DATABASE={'BiinForm_Data_Ingestion'};UID={username133};PWD={password133}'
conn = pyodbc.connect(conn_string133)

cursor = conn.cursor()

conversion_dict = {
    "Tnhh": "TNHH",
    "Mtvt": "MTVT",
    "Cp": "CP",
    "Dntn": "DNTN"
}

cursor.execute("SELECT id, your_column FROM your_table")

for row in cursor.fetchall():
    row_id = row.id
    original_text = row.your_column

    cleaned = clean_html(original_text)
    cleaned = remove_special_start(cleaned)
    cleaned = remove_special_end(cleaned)
    transformed = initcap_and_convert(cleaned, conversion_dict)

    cursor.execute(
        "UPDATE your_table SET your_column = ? WHERE id = ?",
        transformed, row_id
    )

conn.commit()
cursor.close()
conn.close()