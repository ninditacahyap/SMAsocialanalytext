# Social Media Analytics
---

## 1. Package Installation
Ada beberapa package dan library yang harus di siapkan sebelum menjalankan Social Media Analytics. di antarannya :
1. Redis Server 
2. Tesseract-OCR
3. Python
4. Python Libraries
5. libGl

### Redis Installation Windows 10
Untuk menginstall redis di OS Windows bisa di lihat di halaman resmi nya atau bisa di lihat [di sini](https://redis.io/docs/getting-started/installation/install-redis-on-windows/).

### Tesseract-OCR 
Tesseract-OCR digunakan untuk proses ekstraksi data textual pada gambar atau jenis document lainnya.

Untuk installasi Tesseract-OCR bisa di cek [di sini](https:// atesseract-ocr.github.io/tessdoc/Downloads.html)

Atau bisa langsung dengan mengetikkan `sudo apt-get install tesseract-ocr -y` di terminal(linux)

### Python
Untuk Instalasi python bisa di cek [di sini](https://www.python.org/downloads/)

### Python Libraries
**Penting** untuk menginstal semua libraries python di sebuah virtual environment.

Untuk menambahkan sebuah virtual environemt di workspace/folder, bisa menggunakan cara `python -m venv venv` di terminal.

Setelah itu masuk ke virtual environment dengan mengetik `source venv/bin/activate` di terminal  

**Berbeda OS maka beda juga caranya** untuk dokumentasi megenai virtual environment di python bisa di cek [di sini](https://docs.python.org/3/library/venv.html)

Setelah virtual environment aktif, barulah disini kita akan menginstall semua libraries yang di perlukan dengan mengetikkan `python install -r requirements.txt` requirements.txt berisi semua python libraries yang di perlukan beserta semua versi nya. 

### libGl
libGl adalah library yang nantinya akan di gunakan oleh OpenCV untuk menganalisa file photo.

Cara instalasi cukup dengan mengetikkan `sudo apt install libgl1-mesa-glx` di terminal(linux).
**Note**: saat ini kebanyakan library open sources hanya tersedia untuk linux.


---

## 2. Running the Project
Ada beberapa tahapan sebelum menjalankan server flask di project ini diantarannya:

1. Inisiasi Redis Server
2. Inisiasi Worker dengan RQ-Worker
3. Menyiapkan Akun IG untuk proses Scrapping
4. Menjalankan Aplikasi

### Inisasi Redis Server
Setelah menginstall redis server, langkah berikut nya adalah dengan menjalankan redis server terserbut dengan mengetikkan `sudo service redis-server start` di terminal (Untuk OS Windows, harus menjalankannya di WSL)

Untuk mengecek apakah redis server sudah berjalan, bisa dengan mengetikkan perintah `sudo service redis-server status` di terminal WSL(untuk windows).

### Inisiasi Worker dengan RQ-worker
Worker di berperan penting dalam arsitektur aplikasi, karena worker bekerja secara asyncronous di background untuk menghandle proses yang berat dan memakan waktu di server seperti fetching instagram dll.

untuk menjalankan worker, bisa dengan mengetikkan perintah `rq worker sm-worker` di terminal virtual environment berada.

**Catatan:** disini kita akan menggunakan setidaknya 2 terminal atau cmd . satu untuk menjalankan server flask dan satu lagi untuk worker (RQ-worker), ke dua terminal tersebut harus berada di virtual environment karena flask dan rq-worker adalah libraries python yang kita install sebelummnya di venv. jadi ada 2 terminal yang berjalan.

### Menyiapkan akun IG 
akun instagram bisa di isi di file `instascrapper.py` yang berada di folder `scripts`. letaknya ada di fungsi `def login(self, username=(username), password=(password)`.

### Menjalankan Aplikasi
Setelah semua inisialisasi selasai barulah kita bisa menjalankan aplikasi/project dengan mengetikkan `flask run`.

