from flask import Flask, render_template, request, redirect, flash, session, url_for, jsonify, render_template_string
import os
from datetime import datetime
from functools import wraps
from urllib.parse import quote

import mysql.connector
from mysql.connector import Error
from werkzeug.security import generate_password_hash, check_password_hash

try:
    from zoneinfo import ZoneInfo
    TIMEZONE = ZoneInfo("Asia/Jakarta")
except Exception:
    TIMEZONE = None

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-this")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.getenv("COOKIE_SECURE", "false").lower() == "true"

DB_CONFIG = {
    "host": os.getenv("MYSQLHOST") or os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("MYSQLPORT") or os.getenv("DB_PORT", 3306)),
    "user": os.getenv("MYSQLUSER") or os.getenv("DB_USER", "root"),
    "password": os.getenv("MYSQLPASSWORD") or os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("MYSQLDATABASE") or os.getenv("DB_NAME", "rt_03"),
}

WHATSAPP_NUMBER = os.getenv("WHATSAPP_NUMBER", "6285942139246")


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def get_now():
    if TIMEZONE:
        return datetime.now(TIMEZONE)
    return datetime.now()


def get_time_data():
    now = get_now()

    hari_indo = {
        "Monday": "Senin",
        "Tuesday": "Selasa",
        "Wednesday": "Rabu",
        "Thursday": "Kamis",
        "Friday": "Jumat",
        "Saturday": "Sabtu",
        "Sunday": "Minggu",
    }

    bulan_indo = {
        1: "Januari",
        2: "Februari",
        3: "Maret",
        4: "April",
        5: "Mei",
        6: "Juni",
        7: "Juli",
        8: "Agustus",
        9: "September",
        10: "Oktober",
        11: "November",
        12: "Desember",
    }

    hour = now.hour
    if 4 <= hour < 11:
        period = "pagi"
        icon = "sun"
    elif 11 <= hour < 15:
        period = "siang"
        icon = "sun"
    elif 15 <= hour < 18:
        period = "sore"
        icon = "sun"
    else:
        period = "malam"
        icon = "moon"

    return {
        "current_day_name": hari_indo[now.strftime("%A")],
        "current_day": now.day,
        "current_month_name": bulan_indo[now.month],
        "current_year": now.year,
        "current_time_only": now.strftime("%H:%M:%S"),
        "current_period": period,
        "time_icon": icon,
    }


def format_rupiah(value):
    return "{:,.0f}".format(float(value or 0)).replace(",", ".")


def normalize_phone(phone: str) -> str:
    return "".join(ch for ch in (phone or "") if ch.isdigit())


def normalize_name(name: str) -> str:
    return " ".join((name or "").strip().split())


def normalize_text(value: str) -> str:
    return " ".join((value or "").strip().split())


def safe_close(cursor=None, conn=None):
    try:
        if cursor:
            cursor.close()
    except Exception:
        pass
    try:
        if conn and conn.is_connected():
            conn.close()
    except Exception:
        pass


def parse_positive_amount(raw_value: str):
    raw_value = (raw_value or "").strip().replace(".", "").replace(",", ".")
    try:
        amount = float(raw_value)
    except ValueError:
        return None
    if amount <= 0:
        return None
    return amount


def parse_non_negative_int(raw_value, default=0):
    try:
        value = int(str(raw_value).strip())
        return value if value >= 0 else default
    except Exception:
        return default


def get_form_value(source, *keys, default=""):
    for key in keys:
        value = source.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if value != "":
                return value
        else:
            return value
    return default


def clear_old_flashes():
    session.pop("_flashes", None)


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "id_user" not in session:
            flash("Silakan login dulu.", "danger")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "id_user" not in session:
            flash("Silakan login dulu.", "danger")
            return redirect(url_for("login"))
        if session.get("role") != "ketua_rt":
            flash("Akses ditolak.", "danger")
            return redirect(url_for("dashboard_warga"))
        return f(*args, **kwargs)
    return wrapper


def warga_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "id_user" not in session:
            flash("Silakan login dulu.", "danger")
            return redirect(url_for("login"))
        if session.get("role") == "ketua_rt":
            return redirect(url_for("dashboard_admin"))
        return f(*args, **kwargs)
    return wrapper


def column_exists(cursor, table_name, column_name):
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        """,
        (DB_CONFIG["database"], table_name, column_name),
    )
    result = cursor.fetchone()
    return bool(result and result[0] > 0)


def ensure_users_table():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id_user INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(120) NOT NULL,
                gender VARCHAR(20) NOT NULL,
                phone VARCHAR(20) NOT NULL,
                password VARCHAR(255) NOT NULL,
                role ENUM('ketua_rt', 'warga') NOT NULL DEFAULT 'warga',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_phone (phone)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        if not column_exists(cursor, "users", "gender"):
            cursor.execute(
                """
                ALTER TABLE users
                ADD COLUMN gender VARCHAR(20) NOT NULL DEFAULT 'Laki-laki'
                AFTER name
                """
            )

        if not column_exists(cursor, "users", "phone"):
            cursor.execute(
                """
                ALTER TABLE users
                ADD COLUMN phone VARCHAR(20) NOT NULL DEFAULT ''
                AFTER gender
                """
            )

        if not column_exists(cursor, "users", "password"):
            cursor.execute(
                """
                ALTER TABLE users
                ADD COLUMN password VARCHAR(255) NOT NULL DEFAULT ''
                AFTER phone
                """
            )

        if not column_exists(cursor, "users", "role"):
            cursor.execute(
                """
                ALTER TABLE users
                ADD COLUMN role ENUM('ketua_rt', 'warga') NOT NULL DEFAULT 'warga'
                """
            )

        conn.commit()
    finally:
        safe_close(cursor, conn)


def ensure_feedback_table():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS feedback (
                id_feedback INT AUTO_INCREMENT PRIMARY KEY,
                id_user INT NULL,
                nama VARCHAR(120) NOT NULL DEFAULT 'Anonim',
                kategori VARCHAR(100) NOT NULL,
                isi TEXT NOT NULL,
                is_anonymous TINYINT(1) NOT NULL DEFAULT 1,
                status ENUM('baru', 'diproses', 'selesai') NOT NULL DEFAULT 'baru',
                balasan_admin TEXT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        if not column_exists(cursor, "feedback", "is_anonymous"):
            cursor.execute(
                """
                ALTER TABLE feedback
                ADD COLUMN is_anonymous TINYINT(1) NOT NULL DEFAULT 1
                AFTER isi
                """
            )

        if not column_exists(cursor, "feedback", "balasan_admin"):
            cursor.execute(
                """
                ALTER TABLE feedback
                ADD COLUMN balasan_admin TEXT NULL
                AFTER status
                """
            )

        cursor.execute(
            """
            UPDATE feedback
            SET nama = 'Anonim'
            WHERE nama IS NULL OR TRIM(nama) = ''
            """
        )

        conn.commit()
    finally:
        safe_close(cursor, conn)


def ensure_kegiatan_table():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS laporan_kegiatan (
                id_kegiatan INT AUTO_INCREMENT PRIMARY KEY,
                judul_kegiatan VARCHAR(200) NOT NULL,
                tanggal_kegiatan DATE NOT NULL,
                waktu_kegiatan TIME NOT NULL,
                lokasi VARCHAR(255) NOT NULL,
                status ENUM('rencana', 'proses', 'selesai') NOT NULL DEFAULT 'rencana',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        if column_exists(cursor, "laporan_kegiatan", "penanggung_jawab"):
            try:
                cursor.execute(
                    """
                    ALTER TABLE laporan_kegiatan
                    DROP COLUMN penanggung_jawab
                    """
                )
            except Exception:
                pass

        conn.commit()
    finally:
        safe_close(cursor, conn)


def ensure_keuangan_table():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS laporan_keuangan (
                id_keuangan INT AUTO_INCREMENT PRIMARY KEY,
                keterangan VARCHAR(255) NOT NULL,
                tanggal_transaksi DATE NOT NULL,
                jumlah DECIMAL(15,2) NOT NULL,
                jenis ENUM('masuk', 'keluar') NOT NULL,
                kategori VARCHAR(100) NOT NULL DEFAULT 'umum',
                penanggung_jawab VARCHAR(100) NOT NULL DEFAULT '-',
                catatan TEXT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        if not column_exists(cursor, "laporan_keuangan", "tanggal_transaksi"):
            if column_exists(cursor, "laporan_keuangan", "tanggal"):
                cursor.execute(
                    """
                    ALTER TABLE laporan_keuangan
                    CHANGE COLUMN tanggal tanggal_transaksi DATE NOT NULL
                    """
                )
            else:
                cursor.execute(
                    """
                    ALTER TABLE laporan_keuangan
                    ADD COLUMN tanggal_transaksi DATE NOT NULL AFTER keterangan
                    """
                )

        if not column_exists(cursor, "laporan_keuangan", "kategori"):
            cursor.execute(
                """
                ALTER TABLE laporan_keuangan
                ADD COLUMN kategori VARCHAR(100) NOT NULL DEFAULT 'umum' AFTER jenis
                """
            )

        if not column_exists(cursor, "laporan_keuangan", "penanggung_jawab"):
            cursor.execute(
                """
                ALTER TABLE laporan_keuangan
                ADD COLUMN penanggung_jawab VARCHAR(100) NOT NULL DEFAULT '-' AFTER kategori
                """
            )

        if not column_exists(cursor, "laporan_keuangan", "catatan"):
            cursor.execute(
                """
                ALTER TABLE laporan_keuangan
                ADD COLUMN catatan TEXT NULL AFTER penanggung_jawab
                """
            )

        conn.commit()
    finally:
        safe_close(cursor, conn)


def ensure_warga_table():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS warga (
                id_warga INT AUTO_INCREMENT PRIMARY KEY,
                id_user INT NULL,
                nama_lengkap VARCHAR(150) NOT NULL,
                gender VARCHAR(20) NOT NULL DEFAULT 'Laki-laki',
                status_dalam_keluarga ENUM('kepala_keluarga', 'istri', 'anak', 'lainnya') NOT NULL DEFAULT 'lainnya',
                tempat_lahir VARCHAR(100) NULL,
                tanggal_lahir DATE NULL,
                agama VARCHAR(50) NULL,
                status_kepemilikan_rumah ENUM('rumah_sendiri', 'kontrak', 'kos', 'menumpang') NOT NULL DEFAULT 'menumpang',
                jumlah_anak INT NOT NULL DEFAULT 0,
                alamat TEXT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_warga_id_user (id_user),
                CONSTRAINT fk_warga_user
                    FOREIGN KEY (id_user) REFERENCES users(id_user)
                    ON DELETE SET NULL
                    ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        if not column_exists(cursor, "warga", "id_user"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN id_user INT NULL AFTER id_warga
                """
            )

        if not column_exists(cursor, "warga", "nama_lengkap"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN nama_lengkap VARCHAR(150) NOT NULL DEFAULT '' AFTER id_user
                """
            )

        if not column_exists(cursor, "warga", "gender"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN gender VARCHAR(20) NOT NULL DEFAULT 'Laki-laki' AFTER nama_lengkap
                """
            )

        if not column_exists(cursor, "warga", "status_dalam_keluarga"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN status_dalam_keluarga VARCHAR(50) NOT NULL DEFAULT 'lainnya' AFTER gender
                """
            )

        if not column_exists(cursor, "warga", "tempat_lahir"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN tempat_lahir VARCHAR(100) NULL AFTER status_dalam_keluarga
                """
            )

        if not column_exists(cursor, "warga", "tanggal_lahir"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN tanggal_lahir DATE NULL AFTER tempat_lahir
                """
            )

        if not column_exists(cursor, "warga", "agama"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN agama VARCHAR(50) NULL AFTER tanggal_lahir
                """
            )

        if not column_exists(cursor, "warga", "status_kepemilikan_rumah"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN status_kepemilikan_rumah VARCHAR(50) NOT NULL DEFAULT 'menumpang' AFTER agama
                """
            )

        if not column_exists(cursor, "warga", "jumlah_anak"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN jumlah_anak INT NOT NULL DEFAULT 0 AFTER status_kepemilikan_rumah
                """
            )

        if not column_exists(cursor, "warga", "alamat"):
            cursor.execute(
                """
                ALTER TABLE warga
                ADD COLUMN alamat TEXT NULL AFTER jumlah_anak
                """
            )

        conn.commit()
    finally:
        safe_close(cursor, conn)


def bootstrap_tables():
    ensure_users_table()
    ensure_feedback_table()
    ensure_kegiatan_table()
    ensure_keuangan_table()
    ensure_warga_table()


@app.before_request
def boot_app_tables():
    if not getattr(app, "_tables_bootstrapped", False):
        try:
            bootstrap_tables()
            app._tables_bootstrapped = True
        except Exception as e:
            print("BOOTSTRAP TABLE ERROR:", e)
            raise


@app.context_processor
def inject_globals():
    return {"format_rupiah": format_rupiah}


@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.route("/", methods=["GET", "POST"])
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        allow_register_success = request.args.get("notif") == "register_success"
        if not allow_register_success:
            clear_old_flashes()
        return render_template("login.html")

    phone = normalize_phone(get_form_value(request.form, "phone", "no_hp", "nomor_hp", "telp"))
    password = get_form_value(request.form, "password", "pass", "kata_sandi")

    if not phone or not password:
        flash("Nomor HP dan password wajib diisi.", "danger")
        return render_template("login.html")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT *
            FROM users
            WHERE phone = %s
            LIMIT 1
            """,
            (phone,),
        )
        user = cursor.fetchone()

        if user and check_password_hash(user["password"], password):
            session.clear()
            session["id_user"] = user["id_user"]
            session["name"] = user["name"]
            session["role"] = user["role"]
            flash("Login berhasil.", "success")

            if user["role"] == "ketua_rt":
                return redirect(url_for("dashboard_admin"))
            return redirect(url_for("dashboard_warga"))

        flash("Nomor HP atau password salah.", "danger")
    except Exception as e:
        print("LOGIN ERROR:", e)
        flash(f"Error database: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template("login.html")


@app.route("/forgot-password")
def forgot_password():
    wa_number = normalize_phone(WHATSAPP_NUMBER)
    if not wa_number:
        flash("Nomor WhatsApp admin belum diatur.", "danger")
        return redirect(url_for("login"))

    pesan = quote("Halo admin, saya lupa password. Mohon bantu reset password saya.")
    return redirect(f"https://wa.me/{wa_number}?text={pesan}")


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        clear_old_flashes()
        return render_template("register.html")

    name = normalize_name(get_form_value(request.form, "name", "nama", "username"))
    gender = get_form_value(request.form, "gender", "jenis_kelamin", "jk")
    phone = normalize_phone(get_form_value(request.form, "phone", "no_hp", "nomor_hp", "telp"))
    password = get_form_value(request.form, "password", "pass", "kata_sandi")

    if not name or not gender or not phone or not password:
        flash("Semua field wajib diisi.", "danger")
        return render_template("register.html")

    if len(name) < 3:
        flash("Nama minimal 3 karakter.", "danger")
        return render_template("register.html")

    if len(phone) < 10:
        flash("No HP tidak valid.", "danger")
        return render_template("register.html")

    if len(password) < 4:
        flash("Password minimal 4 karakter.", "danger")
        return render_template("register.html")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT id_user FROM users WHERE phone = %s LIMIT 1", (phone,))
        existing_phone = cursor.fetchone()
        if existing_phone:
            flash("No HP sudah terdaftar.", "danger")
            return render_template("register.html")

        cursor.execute(
            """
            SELECT id_user FROM users
            WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))
            LIMIT 1
            """,
            (name,),
        )
        existing_name = cursor.fetchone()
        if existing_name:
            flash("Nama sudah terdaftar. Gunakan nama lain.", "danger")
            return render_template("register.html")

        hashed_password = generate_password_hash(password)
        cursor.execute(
            """
            INSERT INTO users (name, gender, phone, password, role)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (name, gender, phone, hashed_password, "warga"),
        )
        conn.commit()

        flash("Registrasi berhasil. Silakan login.", "success")
        return redirect(url_for("login", notif="register_success"))

    except Exception as e:
        print("REGISTER ERROR:", e)
        flash(f"Error database: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template("register.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Anda berhasil logout.", "success")
    return redirect(url_for("login"))


@app.route("/api/time")
@login_required
def api_time():
    time_data = get_time_data()
    return jsonify(
        {
            "day_name": time_data["current_day_name"],
            "day": time_data["current_day"],
            "month": time_data["current_month_name"],
            "year": time_data["current_year"],
            "time": time_data["current_time_only"],
            "period": time_data["current_period"],
            "icon": time_data["time_icon"],
        }
    )


@app.route("/dashboard/admin")
@admin_required
def dashboard_admin():
    dashboard_stats = {
        "warga": {"total": 0},
        "keuangan": {
            "bulanan": 0,
            "bulanan_fmt": "0",
            "total": 0,
            "total_masuk": 0,
            "total_keluar": 0,
            "saldo": 0,
            "total_masuk_fmt": "0",
            "saldo_fmt": "0",
        },
        "feedback": {"total": 0, "baru": 0, "selesai": 0},
        "kegiatan": {"total": 0, "rencana": 0, "proses": 0, "selesai": 0},
    }

    latest_feedback = []

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM users
            WHERE role = 'warga'
            """
        )
        warga = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'rencana' THEN 1 ELSE 0 END), 0) AS rencana,
                COALESCE(SUM(CASE WHEN status = 'proses' THEN 1 ELSE 0 END), 0) AS proses,
                COALESCE(SUM(CASE WHEN status = 'selesai' THEN 1 ELSE 0 END), 0) AS selesai
            FROM laporan_kegiatan
            """
        )
        kegiatan = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN jenis = 'masuk' THEN jumlah ELSE 0 END), 0) AS total_masuk,
                COALESCE(SUM(CASE WHEN jenis = 'keluar' THEN jumlah ELSE 0 END), 0) AS total_keluar,
                COALESCE(
                    SUM(
                        CASE
                            WHEN jenis = 'masuk'
                                 AND YEAR(tanggal_transaksi) = YEAR(CURDATE())
                                 AND MONTH(tanggal_transaksi) = MONTH(CURDATE())
                            THEN jumlah
                            ELSE 0
                        END
                    ), 0
                ) AS total_bulanan
            FROM laporan_keuangan
            """
        )
        keuangan = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'baru' THEN 1 ELSE 0 END), 0) AS baru,
                COALESCE(SUM(CASE WHEN status = 'selesai' THEN 1 ELSE 0 END), 0) AS selesai
            FROM feedback
            """
        )
        feedback = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM warga
            """
        )
        warga_table_count = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT id_feedback, nama, kategori, isi, status, balasan_admin, created_at, is_anonymous
            FROM feedback
            ORDER BY id_feedback DESC
            LIMIT 3
            """
        )
        latest_feedback = cursor.fetchall() or []

        for item in latest_feedback:
            if item.get("created_at"):
                item["created_at_fmt"] = item["created_at"].strftime("%d-%m-%Y %H:%M")
            else:
                item["created_at_fmt"] = "-"

            nama_feedback = item.get("nama") or "Anonim"
            item["display_name"] = "Anonim" if item.get("is_anonymous") else nama_feedback
            item["initial"] = (item["display_name"][:1] or "A").upper()

        dashboard_stats["warga"] = {
            "total": int(warga_table_count.get("total", warga.get("total", 0)) or 0),
        }

        dashboard_stats["kegiatan"] = {
            "total": int(kegiatan.get("total", 0) or 0),
            "rencana": int(kegiatan.get("rencana", 0) or 0),
            "proses": int(kegiatan.get("proses", 0) or 0),
            "selesai": int(kegiatan.get("selesai", 0) or 0),
        }

        total_masuk = float(keuangan.get("total_masuk", 0) or 0)
        total_keluar = float(keuangan.get("total_keluar", 0) or 0)
        total_bulanan = float(keuangan.get("total_bulanan", 0) or 0)
        saldo = total_masuk - total_keluar

        dashboard_stats["keuangan"] = {
            "bulanan": total_bulanan,
            "bulanan_fmt": format_rupiah(total_bulanan),
            "total": int(keuangan.get("total", 0) or 0),
            "total_masuk": total_masuk,
            "total_keluar": total_keluar,
            "saldo": saldo,
            "total_masuk_fmt": format_rupiah(total_masuk),
            "saldo_fmt": format_rupiah(saldo),
        }

        dashboard_stats["feedback"] = {
            "total": int(feedback.get("total", 0) or 0),
            "baru": int(feedback.get("baru", 0) or 0),
            "selesai": int(feedback.get("selesai", 0) or 0),
        }

    except Exception as e:
        print("DASHBOARD ADMIN ERROR:", e)
        flash(f"Gagal memuat rekap dashboard: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "dashboard_admin.html",
        name=session["name"],
        role=session["role"],
        dashboard_stats=dashboard_stats,
        latest_feedback=latest_feedback,
        **get_time_data(),
    )


@app.route("/dashboard/warga")
@warga_required
def dashboard_warga():
    feedback_user = []
    kegiatan_terbaru = []
    total_feedback = 0
    summary_keuangan = {
        "total_data": 0,
        "total_masuk": 0,
        "total_keluar": 0,
        "saldo": 0,
        "total_masuk_fmt": "0",
        "total_keluar_fmt": "0",
        "saldo_fmt": "0",
    }

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT id_feedback, kategori, isi, status, balasan_admin, created_at, is_anonymous
            FROM feedback
            WHERE id_user = %s
            ORDER BY id_feedback DESC
            LIMIT 10
            """,
            (session["id_user"],),
        )
        feedback_user = cursor.fetchall() or []

        for item in feedback_user:
            if item.get("created_at"):
                item["created_at_fmt"] = item["created_at"].strftime("%d-%m-%Y %H:%M")
            else:
                item["created_at_fmt"] = "-"

        total_feedback = len(feedback_user)

        cursor.execute(
            """
            SELECT judul_kegiatan, tanggal_kegiatan, waktu_kegiatan, lokasi, status
            FROM laporan_kegiatan
            ORDER BY tanggal_kegiatan DESC, waktu_kegiatan DESC, id_kegiatan DESC
            LIMIT 5
            """
        )
        kegiatan_terbaru = cursor.fetchall() or []

        for item in kegiatan_terbaru:
            if item.get("tanggal_kegiatan"):
                item["tanggal_kegiatan"] = item["tanggal_kegiatan"].strftime("%d-%m-%Y")
            if item.get("waktu_kegiatan"):
                item["waktu_kegiatan"] = str(item["waktu_kegiatan"])[:5]

        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_data,
                COALESCE(SUM(CASE WHEN jenis = 'masuk' THEN jumlah ELSE 0 END), 0) AS total_masuk,
                COALESCE(SUM(CASE WHEN jenis = 'keluar' THEN jumlah ELSE 0 END), 0) AS total_keluar
            FROM laporan_keuangan
            """
        )
        keuangan = cursor.fetchone() or {}

        total_masuk = float(keuangan.get("total_masuk", 0) or 0)
        total_keluar = float(keuangan.get("total_keluar", 0) or 0)
        saldo = total_masuk - total_keluar

        summary_keuangan = {
            "total_data": int(keuangan.get("total_data", 0) or 0),
            "total_masuk": total_masuk,
            "total_keluar": total_keluar,
            "saldo": saldo,
            "total_masuk_fmt": format_rupiah(total_masuk),
            "total_keluar_fmt": format_rupiah(total_keluar),
            "saldo_fmt": format_rupiah(saldo),
        }

    except Exception as e:
        print("DASHBOARD WARGA ERROR:", e)
        flash(f"Gagal memuat dashboard warga: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "dashboard_warga.html",
        name=session["name"],
        role=session["role"],
        feedback_user=feedback_user,
        total_feedback=total_feedback,
        kegiatan_terbaru=kegiatan_terbaru,
        summary_keuangan=summary_keuangan,
        **get_time_data(),
    )


@app.route("/feedback-warga")
@warga_required
def feedback_warga():
    feedback_user = []
    total_feedback = 0

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT id_feedback, kategori, isi, status, balasan_admin, created_at, is_anonymous
            FROM feedback
            WHERE id_user = %s
            ORDER BY id_feedback DESC
            LIMIT 10
            """,
            (session["id_user"],),
        )
        feedback_user = cursor.fetchall() or []

        for item in feedback_user:
            if item.get("created_at"):
                item["created_at_fmt"] = item["created_at"].strftime("%d-%m-%Y %H:%M")
            else:
                item["created_at_fmt"] = "-"

        total_feedback = len(feedback_user)

    except Exception as e:
        print("FEEDBACK WARGA ERROR:", e)
        flash(f"Gagal memuat halaman feedback warga: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "feedback_warga.html",
        name=session["name"],
        role=session["role"],
        feedback_user=feedback_user,
        total_feedback=total_feedback,
        **get_time_data(),
    )


@app.route("/feedback/create", methods=["POST"])
@warga_required
def create_feedback():
    kategori = get_form_value(request.form, "kategori", "category")
    isi = get_form_value(request.form, "isi", "pesan", "message")

    anonymous_raw = get_form_value(request.form, "is_anonymous", default="1").lower()
    is_anonymous = 0 if anonymous_raw in {"0", "false", "off", "tidak"} else 1

    if not kategori or not isi:
        flash("Kategori dan isi feedback wajib diisi.", "danger")
        return redirect(url_for("feedback_warga"))

    nama_pengirim = "Anonim" if is_anonymous else session["name"]

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO feedback (id_user, nama, kategori, isi, is_anonymous, status)
            VALUES (%s, %s, %s, %s, %s, 'baru')
            """,
            (session["id_user"], nama_pengirim, kategori, isi, is_anonymous),
        )
        conn.commit()
        flash("Feedback berhasil dikirim ke admin.", "success")
    except Exception as e:
        print("CREATE FEEDBACK ERROR:", e)
        flash(f"Gagal mengirim feedback: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("feedback_warga"))


@app.route("/feedback-admin")
@admin_required
def feedback_admin():
    feedback_list = []

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT id_feedback, nama, kategori, isi, status, balasan_admin, created_at, is_anonymous
            FROM feedback
            ORDER BY id_feedback DESC
            """
        )
        feedback_list = cursor.fetchall() or []

        for item in feedback_list:
            if item.get("created_at"):
                item["created_at_fmt"] = item["created_at"].strftime("%d-%m-%Y %H:%M")
            else:
                item["created_at_fmt"] = "-"

            nama_feedback = item.get("nama") or "Anonim"
            item["display_name"] = "Anonim" if item.get("is_anonymous") else nama_feedback
            item["initial"] = (item["display_name"][:1] or "A").upper()

    except Exception as e:
        print("FEEDBACK ADMIN ERROR:", e)
        flash(f"Gagal memuat halaman feedback admin: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "feedback_admin.html",
        name=session["name"],
        role=session["role"],
        feedback_list=feedback_list,
        **get_time_data(),
    )


@app.route("/feedback-admin/update/<int:id_feedback>", methods=["POST"])
@admin_required
def update_feedback_admin(id_feedback):
    status = get_form_value(request.form, "status")
    balasan_admin = get_form_value(request.form, "balasan_admin")

    allowed_status = {"baru", "diproses", "selesai"}
    if status not in allowed_status:
        flash("Status feedback tidak valid.", "danger")
        return redirect(url_for("feedback_admin"))

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE feedback
            SET status = %s,
                balasan_admin = %s
            WHERE id_feedback = %s
            """,
            (status, balasan_admin, id_feedback),
        )
        conn.commit()
        flash("Feedback berhasil diperbarui.", "success")
    except Exception as e:
        print("UPDATE FEEDBACK ADMIN ERROR:", e)
        flash(f"Gagal memperbarui feedback: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("feedback_admin"))


@app.route("/kegiatan-admin")
@admin_required
def kegiatan_admin():
    kegiatan_list = []
    edit_data = None
    edit_id = request.args.get("edit", type=int)

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT
                id_kegiatan,
                judul_kegiatan,
                tanggal_kegiatan,
                waktu_kegiatan,
                lokasi,
                status
            FROM laporan_kegiatan
            ORDER BY tanggal_kegiatan DESC, waktu_kegiatan DESC, id_kegiatan DESC
            """
        )
        kegiatan_list = cursor.fetchall() or []

        for item in kegiatan_list:
            if item.get("tanggal_kegiatan"):
                item["tanggal_fmt"] = item["tanggal_kegiatan"].strftime("%d-%m-%Y")
                item["tanggal_input"] = item["tanggal_kegiatan"].strftime("%Y-%m-%d")
            else:
                item["tanggal_fmt"] = "-"
                item["tanggal_input"] = ""

            if item.get("waktu_kegiatan"):
                item["waktu_fmt"] = str(item["waktu_kegiatan"])[:5]
                item["waktu_input"] = str(item["waktu_kegiatan"])[:5]
            else:
                item["waktu_fmt"] = "-"
                item["waktu_input"] = ""

            item["judul_display"] = item.get("judul_kegiatan") or "-"
            item["lokasi_display"] = item.get("lokasi") or "-"
            item["status_display"] = (item.get("status") or "-").strip().lower()

            if edit_id and item["id_kegiatan"] == edit_id:
                edit_data = item

    except Exception as e:
        print("KEGIATAN ADMIN ERROR:", e)
        flash(f"Gagal memuat halaman kegiatan admin: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "laporan_kegiatan_admin.html",
        name=session["name"],
        role=session["role"],
        kegiatan_list=kegiatan_list,
        edit_data=edit_data,
        **get_time_data(),
    )


@app.route("/kegiatan-admin/create", methods=["POST"])
@admin_required
def create_kegiatan_admin():
    judul_kegiatan = get_form_value(request.form, "judul_kegiatan", "judul")
    tanggal_kegiatan = get_form_value(request.form, "tanggal_kegiatan", "tanggal")
    waktu_kegiatan = get_form_value(request.form, "waktu_kegiatan", "waktu")
    lokasi = get_form_value(request.form, "lokasi")
    status = get_form_value(request.form, "status", default="rencana").lower()

    if not judul_kegiatan or not tanggal_kegiatan or not waktu_kegiatan or not lokasi:
        flash("Semua field kegiatan wajib diisi.", "danger")
        return redirect(url_for("kegiatan_admin"))

    if status not in {"rencana", "proses", "selesai"}:
        flash("Status kegiatan tidak valid.", "danger")
        return redirect(url_for("kegiatan_admin"))

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO laporan_kegiatan
            (judul_kegiatan, tanggal_kegiatan, waktu_kegiatan, lokasi, status)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (judul_kegiatan, tanggal_kegiatan, waktu_kegiatan, lokasi, status),
        )
        conn.commit()

        if cursor.rowcount < 1:
            flash("Data kegiatan gagal disimpan.", "danger")
        else:
            flash("Data kegiatan berhasil ditambahkan.", "success")

    except Error as e:
        print("CREATE KEGIATAN DB ERROR:", e)
        flash(f"Gagal menambah kegiatan: {e}", "danger")
    except Exception as e:
        print("CREATE KEGIATAN ERROR:", e)
        flash(f"Gagal menambah kegiatan: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("kegiatan_admin"))


@app.route("/kegiatan-admin/update/<int:id_kegiatan>", methods=["POST"])
@admin_required
def update_kegiatan_admin(id_kegiatan):
    judul_kegiatan = get_form_value(request.form, "judul_kegiatan", "judul")
    tanggal_kegiatan = get_form_value(request.form, "tanggal_kegiatan", "tanggal")
    waktu_kegiatan = get_form_value(request.form, "waktu_kegiatan", "waktu")
    lokasi = get_form_value(request.form, "lokasi")
    status = get_form_value(request.form, "status", default="rencana").lower()

    if not judul_kegiatan or not tanggal_kegiatan or not waktu_kegiatan or not lokasi:
        flash("Data update kegiatan tidak valid.", "danger")
        return redirect(url_for("kegiatan_admin", edit=id_kegiatan))

    if status not in {"rencana", "proses", "selesai"}:
        flash("Status kegiatan tidak valid.", "danger")
        return redirect(url_for("kegiatan_admin", edit=id_kegiatan))

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE laporan_kegiatan
            SET judul_kegiatan = %s,
                tanggal_kegiatan = %s,
                waktu_kegiatan = %s,
                lokasi = %s,
                status = %s
            WHERE id_kegiatan = %s
            """,
            (judul_kegiatan, tanggal_kegiatan, waktu_kegiatan, lokasi, status, id_kegiatan),
        )
        conn.commit()
        flash("Data kegiatan berhasil diperbarui.", "success")
    except Exception as e:
        print("UPDATE KEGIATAN ERROR:", e)
        flash(f"Gagal memperbarui kegiatan: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("kegiatan_admin"))


@app.route("/kegiatan-admin/delete/<int:id_kegiatan>", methods=["POST"])
@admin_required
def delete_kegiatan_admin(id_kegiatan):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM laporan_kegiatan WHERE id_kegiatan = %s",
            (id_kegiatan,),
        )
        conn.commit()
        flash("Data kegiatan berhasil dihapus.", "success")
    except Exception as e:
        print("DELETE KEGIATAN ERROR:", e)
        flash(f"Gagal menghapus kegiatan: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("kegiatan_admin"))


@app.route("/laporan-kegiatan-warga")
@warga_required
def laporan_kegiatan_warga():
    kegiatan_list = []

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT
                id_kegiatan,
                judul_kegiatan,
                tanggal_kegiatan,
                waktu_kegiatan,
                lokasi,
                status
            FROM laporan_kegiatan
            ORDER BY tanggal_kegiatan DESC, waktu_kegiatan DESC, id_kegiatan DESC
            """
        )
        kegiatan_list = cursor.fetchall() or []

        for item in kegiatan_list:
            if item.get("tanggal_kegiatan"):
                item["tanggal_fmt"] = item["tanggal_kegiatan"].strftime("%d-%m-%Y")
            else:
                item["tanggal_fmt"] = "-"

            if item.get("waktu_kegiatan"):
                item["waktu_fmt"] = str(item["waktu_kegiatan"])[:5]
            else:
                item["waktu_fmt"] = "-"

            item["judul_display"] = item.get("judul_kegiatan") or "-"
            item["lokasi_display"] = item.get("lokasi") or "-"
            item["status_display"] = (item.get("status") or "-").strip().lower()

    except Exception as e:
        print("KEGIATAN WARGA ERROR:", e)
        flash(f"Gagal memuat laporan kegiatan warga: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "laporan_kegiatan_warga.html",
        name=session["name"],
        role=session["role"],
        kegiatan_list=kegiatan_list,
        **get_time_data(),
    )


@app.route("/keuangan-admin")
@admin_required
def keuangan_admin():
    keuangan_list = []
    edit_data = None
    edit_id = request.args.get("edit", type=int)
    summary = {
        "total_data": 0,
        "total_masuk": 0,
        "total_keluar": 0,
        "saldo": 0,
        "total_masuk_fmt": "0",
        "total_keluar_fmt": "0",
        "saldo_fmt": "0",
    }

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT
                id_keuangan,
                keterangan,
                tanggal_transaksi,
                jenis,
                jumlah
            FROM laporan_keuangan
            ORDER BY tanggal_transaksi DESC, id_keuangan DESC
            """
        )
        keuangan_list = cursor.fetchall() or []

        total_masuk = 0
        total_keluar = 0

        for item in keuangan_list:
            if item.get("tanggal_transaksi"):
                item["tanggal_fmt"] = item["tanggal_transaksi"].strftime("%d-%m-%Y")
                item["tanggal_input"] = item["tanggal_transaksi"].strftime("%Y-%m-%d")
            else:
                item["tanggal_fmt"] = "-"
                item["tanggal_input"] = ""

            item["keterangan_display"] = item.get("keterangan") or "-"
            item["jenis_display"] = (item.get("jenis") or "-").strip().lower()

            jumlah = float(item.get("jumlah", 0) or 0)
            item["jumlah_value"] = jumlah
            item["jumlah_fmt"] = format_rupiah(jumlah)

            if item["jenis_display"] == "masuk":
                total_masuk += jumlah
            elif item["jenis_display"] == "keluar":
                total_keluar += jumlah

            if edit_id and item["id_keuangan"] == edit_id:
                edit_data = item

        saldo = total_masuk - total_keluar

        summary = {
            "total_data": len(keuangan_list),
            "total_masuk": total_masuk,
            "total_keluar": total_keluar,
            "saldo": saldo,
            "total_masuk_fmt": format_rupiah(total_masuk),
            "total_keluar_fmt": format_rupiah(total_keluar),
            "saldo_fmt": format_rupiah(saldo),
        }

    except Exception as e:
        print("KEUANGAN ADMIN ERROR:", e)
        flash(f"Gagal memuat halaman keuangan admin: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "laporan_keuangan_admin.html",
        name=session["name"],
        role=session["role"],
        keuangan_list=keuangan_list,
        summary=summary,
        edit_data=edit_data,
        **get_time_data(),
    )


@app.route("/keuangan-admin/create", methods=["POST"])
@admin_required
def create_keuangan_admin():
    tanggal = get_form_value(request.form, "tanggal")
    keterangan = get_form_value(request.form, "keterangan")
    jenis = get_form_value(request.form, "jenis").lower()
    jumlah = parse_positive_amount(get_form_value(request.form, "jumlah"))

    if not tanggal or not keterangan or jenis not in {"masuk", "keluar"} or jumlah is None:
        flash("Data keuangan tidak valid. Pastikan semua field terisi benar.", "danger")
        return redirect(url_for("keuangan_admin"))

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO laporan_keuangan
            (keterangan, tanggal_transaksi, jumlah, jenis, kategori, penanggung_jawab, catatan)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (keterangan, tanggal, jumlah, jenis, "umum", "-", None),
        )
        conn.commit()
        flash("Data keuangan berhasil ditambahkan.", "success")
    except Exception as e:
        print("CREATE KEUANGAN ERROR:", e)
        flash(f"Gagal menambah data keuangan: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("keuangan_admin"))


@app.route("/keuangan-admin/update/<int:id_keuangan>", methods=["POST"])
@admin_required
def update_keuangan_admin(id_keuangan):
    tanggal = get_form_value(request.form, "tanggal")
    keterangan = get_form_value(request.form, "keterangan")
    jenis = get_form_value(request.form, "jenis").lower()
    jumlah = parse_positive_amount(get_form_value(request.form, "jumlah"))

    if not tanggal or not keterangan or jenis not in {"masuk", "keluar"} or jumlah is None:
        flash("Data update keuangan tidak valid.", "danger")
        return redirect(url_for("keuangan_admin", edit=id_keuangan))

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE laporan_keuangan
            SET keterangan = %s,
                tanggal_transaksi = %s,
                jenis = %s,
                jumlah = %s
            WHERE id_keuangan = %s
            """,
            (keterangan, tanggal, jenis, jumlah, id_keuangan),
        )
        conn.commit()
        flash("Data keuangan berhasil diperbarui.", "success")
    except Exception as e:
        print("UPDATE KEUANGAN ERROR:", e)
        flash(f"Gagal mengupdate data keuangan: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("keuangan_admin"))


@app.route("/keuangan-admin/delete/<int:id_keuangan>", methods=["POST"])
@admin_required
def delete_keuangan_admin(id_keuangan):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM laporan_keuangan WHERE id_keuangan = %s",
            (id_keuangan,),
        )
        conn.commit()
        flash("Data keuangan berhasil dihapus.", "success")
    except Exception as e:
        print("DELETE KEUANGAN ERROR:", e)
        flash(f"Gagal menghapus data keuangan: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("keuangan_admin"))


@app.route("/laporan-keuangan-warga")
@warga_required
def laporan_keuangan_warga():
    keuangan_list = []
    summary = {
        "total_data": 0,
        "total_masuk": 0,
        "total_keluar": 0,
        "saldo": 0,
        "total_masuk_fmt": "0",
        "total_keluar_fmt": "0",
        "saldo_fmt": "0",
    }

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT
                id_keuangan,
                keterangan,
                tanggal_transaksi,
                jenis,
                jumlah
            FROM laporan_keuangan
            ORDER BY tanggal_transaksi DESC, id_keuangan DESC
            """
        )
        keuangan_list = cursor.fetchall() or []

        total_masuk = 0
        total_keluar = 0

        for item in keuangan_list:
            if item.get("tanggal_transaksi"):
                item["tanggal_fmt"] = item["tanggal_transaksi"].strftime("%d-%m-%Y")
            else:
                item["tanggal_fmt"] = "-"

            item["keterangan_display"] = item.get("keterangan") or "-"
            item["jenis_display"] = (item.get("jenis") or "-").strip().lower()

            jumlah = float(item.get("jumlah", 0) or 0)
            item["jumlah_value"] = jumlah
            item["jumlah_fmt"] = format_rupiah(jumlah)

            if item["jenis_display"] == "masuk":
                total_masuk += jumlah
            elif item["jenis_display"] == "keluar":
                total_keluar += jumlah

        saldo = total_masuk - total_keluar

        summary = {
            "total_data": len(keuangan_list),
            "total_masuk": total_masuk,
            "total_keluar": total_keluar,
            "saldo": saldo,
            "total_masuk_fmt": format_rupiah(total_masuk),
            "total_keluar_fmt": format_rupiah(total_keluar),
            "saldo_fmt": format_rupiah(saldo),
        }

    except Exception as e:
        print("KEUANGAN WARGA ERROR:", e)
        flash(f"Gagal memuat laporan keuangan warga: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "laporan_keuangan_warga.html",
        name=session["name"],
        role=session["role"],
        keuangan_list=keuangan_list,
        summary=summary,
        **get_time_data(),
    )


WARGA_FORM_TEMPLATE = """
<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        * { box-sizing: border-box; font-family: Arial, sans-serif; }
        body { margin: 0; background: #f4f6fb; color: #1f2937; }
        .wrap { max-width: 760px; margin: 32px auto; padding: 0 16px; }
        .card {
            background: #fff;
            border-radius: 18px;
            box-shadow: 0 10px 30px rgba(0,0,0,.08);
            padding: 24px;
        }
        h1 { margin: 0 0 20px; font-size: 24px; }
        .grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 14px;
        }
        .full { grid-column: 1 / -1; }
        label {
            display: block;
            font-size: 13px;
            font-weight: 700;
            margin-bottom: 6px;
        }
        input, select, textarea {
            width: 100%;
            border: 1px solid #d1d5db;
            border-radius: 12px;
            padding: 12px 14px;
            font-size: 14px;
            outline: none;
            background: #fff;
        }
        textarea { min-height: 110px; resize: vertical; }
        .actions {
            margin-top: 20px;
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
        }
        .btn {
            border: none;
            border-radius: 12px;
            padding: 12px 18px;
            font-size: 14px;
            font-weight: 700;
            cursor: pointer;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }
        .btn-primary { background: #2f3b54; color: #fff; }
        .btn-secondary { background: #e5e7eb; color: #111827; }
        .flash-wrap { margin-bottom: 16px; display: grid; gap: 10px; }
        .flash {
            padding: 12px 14px;
            border-radius: 12px;
            font-size: 14px;
            font-weight: 600;
        }
        .flash-danger { background: #fee2e2; color: #991b1b; }
        .flash-success { background: #dcfce7; color: #166534; }
        .flash-warning { background: #fef3c7; color: #92400e; }
        .muted { font-size: 12px; color: #6b7280; margin-top: 6px; }
        @media (max-width: 640px) {
            .grid { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="wrap">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                <div class="flash-wrap">
                    {% for category, message in messages %}
                        <div class="flash flash-{{ category }}">{{ message }}</div>
                    {% endfor %}
                </div>
            {% endif %}
        {% endwith %}

        <div class="card">
            <h1>{{ title }}</h1>

            <form method="POST">
                <div class="grid">
                    <div class="full">
                        <label for="id_user">Akun User Terkait</label>
                        <select name="id_user" id="id_user">
                            <option value="">-- Tidak dikaitkan ke akun user --</option>
                            {% for u in users_list %}
                                <option value="{{ u.id_user }}" {% if form_data.id_user|string == u.id_user|string %}selected{% endif %}>
                                    {{ u.name }} - {{ u.phone }}
                                </option>
                            {% endfor %}
                        </select>
                        <div class="muted">Opsional. Pilih jika data warga ingin dikaitkan ke akun login yang sudah ada.</div>
                    </div>

                    <div class="full">
                        <label for="nama_lengkap">Nama Lengkap</label>
                        <input type="text" name="nama_lengkap" id="nama_lengkap" value="{{ form_data.nama_lengkap or '' }}" required>
                    </div>

                    <div>
                        <label for="gender">Jenis Kelamin</label>
                        <select name="gender" id="gender" required>
                            <option value="Laki-laki" {% if form_data.gender == 'Laki-laki' %}selected{% endif %}>Laki-laki</option>
                            <option value="Perempuan" {% if form_data.gender == 'Perempuan' %}selected{% endif %}>Perempuan</option>
                        </select>
                    </div>

                    <div>
                        <label for="status_dalam_keluarga">Status Dalam Keluarga</label>
                        <select name="status_dalam_keluarga" id="status_dalam_keluarga" required>
                            <option value="kepala_keluarga" {% if form_data.status_dalam_keluarga == 'kepala_keluarga' %}selected{% endif %}>Kepala Keluarga</option>
                            <option value="istri" {% if form_data.status_dalam_keluarga == 'istri' %}selected{% endif %}>Istri</option>
                            <option value="anak" {% if form_data.status_dalam_keluarga == 'anak' %}selected{% endif %}>Anak</option>
                            <option value="lainnya" {% if form_data.status_dalam_keluarga == 'lainnya' %}selected{% endif %}>Lainnya</option>
                        </select>
                    </div>

                    <div>
                        <label for="tempat_lahir">Tempat Lahir</label>
                        <input type="text" name="tempat_lahir" id="tempat_lahir" value="{{ form_data.tempat_lahir or '' }}">
                    </div>

                    <div>
                        <label for="tanggal_lahir">Tanggal Lahir</label>
                        <input type="date" name="tanggal_lahir" id="tanggal_lahir" value="{{ form_data.tanggal_lahir or '' }}">
                    </div>

                    <div>
                        <label for="agama">Agama</label>
                        <select name="agama" id="agama" required>
                            <option value="">Pilih Agama</option>
                            <option value="Islam" {% if form_data.agama == 'Islam' %}selected{% endif %}>Islam</option>
                            <option value="Kristen" {% if form_data.agama == 'Kristen' %}selected{% endif %}>Kristen</option>
                            <option value="Katolik" {% if form_data.agama == 'Katolik' %}selected{% endif %}>Katolik</option>
                            <option value="Hindu" {% if form_data.agama == 'Hindu' %}selected{% endif %}>Hindu</option>
                            <option value="Buddha" {% if form_data.agama == 'Buddha' %}selected{% endif %}>Buddha</option>
                            <option value="Konghucu" {% if form_data.agama == 'Konghucu' %}selected{% endif %}>Konghucu</option>
                        </select>
                    </div>

                    <div>
                        <label for="status_kepemilikan_rumah">Status Kepemilikan Rumah</label>
                        <select name="status_kepemilikan_rumah" id="status_kepemilikan_rumah" required>
                            <option value="rumah_sendiri" {% if form_data.status_kepemilikan_rumah == 'rumah_sendiri' %}selected{% endif %}>Rumah Sendiri</option>
                            <option value="kontrak" {% if form_data.status_kepemilikan_rumah == 'kontrak' %}selected{% endif %}>Kontrak</option>
                            <option value="kos" {% if form_data.status_kepemilikan_rumah == 'kos' %}selected{% endif %}>Kos</option>
                            <option value="menumpang" {% if form_data.status_kepemilikan_rumah == 'menumpang' %}selected{% endif %}>Menumpang</option>
                        </select>
                    </div>

                    <div>
                        <label for="jumlah_anak">Jumlah Anak</label>
                        <input type="number" min="0" name="jumlah_anak" id="jumlah_anak" value="{{ form_data.jumlah_anak or 0 }}" required>
                    </div>

                    <div class="full">
                        <label for="alamat">Alamat</label>
                        <textarea name="alamat" id="alamat">{{ form_data.alamat or '' }}</textarea>
                    </div>
                </div>

                <div class="actions">
                    <button type="submit" class="btn btn-primary">{{ submit_label }}</button>
                    <a href="{{ url_for('warga_admin') }}" class="btn btn-secondary">Kembali</a>
                </div>
            </form>
        </div>
    </div>
</body>
</html>
"""


def get_warga_form_data(source):
    allowed_gender = {"Laki-laki", "Perempuan"}
    allowed_status_keluarga = {"kepala_keluarga", "istri", "anak", "lainnya"}
    allowed_status_rumah = {"rumah_sendiri", "kontrak", "kos", "menumpang"}
    allowed_agama = {"Islam", "Kristen", "Katolik", "Hindu", "Buddha", "Konghucu"}

    id_user_raw = get_form_value(source, "id_user")
    try:
        id_user = int(id_user_raw) if str(id_user_raw).strip() != "" else None
    except Exception:
        id_user = None

    gender = get_form_value(source, "gender", default="Laki-laki")
    if gender not in allowed_gender:
        gender = "Laki-laki"

    status_dalam_keluarga = get_form_value(source, "status_dalam_keluarga", default="lainnya")
    if status_dalam_keluarga not in allowed_status_keluarga:
        status_dalam_keluarga = "lainnya"

    status_kepemilikan_rumah = get_form_value(source, "status_kepemilikan_rumah", default="menumpang")
    if status_kepemilikan_rumah not in allowed_status_rumah:
        status_kepemilikan_rumah = "menumpang"

    agama = get_form_value(source, "agama")
    if agama not in allowed_agama:
        agama = ""

    return {
        "id_user": id_user,
        "nama_lengkap": normalize_name(get_form_value(source, "nama_lengkap")),
        "gender": gender,
        "status_dalam_keluarga": status_dalam_keluarga,
        "tempat_lahir": normalize_text(get_form_value(source, "tempat_lahir")),
        "tanggal_lahir": get_form_value(source, "tanggal_lahir"),
        "agama": agama,
        "status_kepemilikan_rumah": status_kepemilikan_rumah,
        "jumlah_anak": parse_non_negative_int(get_form_value(source, "jumlah_anak", default="0")),
        "alamat": normalize_text(get_form_value(source, "alamat")),
    }


def get_users_warga_options():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT id_user, name, phone
            FROM users
            WHERE role = 'warga'
            ORDER BY name ASC
            """
        )
        return cursor.fetchall() or []
    except Exception as e:
        print("GET USERS WARGA OPTIONS ERROR:", e)
        return []
    finally:
        safe_close(cursor, conn)


def render_warga_form_page(title, submit_label, form_data):
    return render_template_string(
        WARGA_FORM_TEMPLATE,
        title=title,
        submit_label=submit_label,
        form_data=form_data,
        users_list=get_users_warga_options(),
    )


@app.route("/warga-admin")
@admin_required
def warga_admin():
    warga_list = []

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT
                w.id_warga,
                w.id_user,
                w.nama_lengkap,
                w.gender,
                w.status_dalam_keluarga,
                w.tempat_lahir,
                w.tanggal_lahir,
                w.agama,
                w.status_kepemilikan_rumah,
                w.jumlah_anak,
                w.alamat,
                u.phone
            FROM warga w
            LEFT JOIN users u ON u.id_user = w.id_user
            ORDER BY w.nama_lengkap ASC
            """
        )
        warga_list = cursor.fetchall() or []

        for item in warga_list:
            if item.get("tanggal_lahir"):
                item["tanggal_lahir_fmt"] = item["tanggal_lahir"].strftime("%d-%m-%Y")
                item["tanggal_lahir_iso"] = item["tanggal_lahir"].strftime("%Y-%m-%d")
            else:
                item["tanggal_lahir_fmt"] = "-"
                item["tanggal_lahir_iso"] = ""

            item["nama_lengkap"] = item.get("nama_lengkap") or "-"
            item["gender"] = item.get("gender") or "-"
            item["status_dalam_keluarga"] = item.get("status_dalam_keluarga") or "lainnya"
            item["tempat_lahir"] = item.get("tempat_lahir") or "-"
            item["agama"] = item.get("agama") or "-"
            item["status_kepemilikan_rumah"] = item.get("status_kepemilikan_rumah") or "-"
            item["jumlah_anak"] = int(item.get("jumlah_anak", 0) or 0)
            item["alamat"] = item.get("alamat") or "-"
            item["phone"] = item.get("phone") or "-"

    except Exception as e:
        print("WARGA ADMIN ERROR:", e)
        flash(f"Gagal memuat data warga: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return render_template(
        "warga_admin.html",
        name=session["name"],
        role=session["role"],
        warga_list=warga_list,
        **get_time_data(),
    )


@app.route("/warga-admin/create", methods=["GET", "POST"])
@admin_required
def create_warga_admin():
    if request.method == "GET":
        return render_warga_form_page(
            "Tambah Data Warga",
            "Simpan Data",
            {
                "id_user": None,
                "nama_lengkap": "",
                "gender": "Laki-laki",
                "status_dalam_keluarga": "lainnya",
                "tempat_lahir": "",
                "tanggal_lahir": "",
                "agama": "",
                "status_kepemilikan_rumah": "menumpang",
                "jumlah_anak": 0,
                "alamat": "",
            },
        )

    form_data = get_warga_form_data(request.form)

    if not form_data["nama_lengkap"]:
        flash("Nama lengkap wajib diisi.", "danger")
        return render_warga_form_page("Tambah Data Warga", "Simpan Data", form_data)

    if not form_data["agama"]:
        flash("Agama wajib dipilih.", "danger")
        return render_warga_form_page("Tambah Data Warga", "Simpan Data", form_data)

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        if form_data["id_user"] is not None:
            cursor.execute(
                "SELECT id_user FROM users WHERE id_user = %s LIMIT 1",
                (form_data["id_user"],),
            )
            user_row = cursor.fetchone()
            if not user_row:
                flash("User yang dipilih tidak ditemukan.", "danger")
                return render_warga_form_page("Tambah Data Warga", "Simpan Data", form_data)

        cursor.execute(
            """
            INSERT INTO warga (
                id_user,
                nama_lengkap,
                gender,
                status_dalam_keluarga,
                tempat_lahir,
                tanggal_lahir,
                agama,
                status_kepemilikan_rumah,
                jumlah_anak,
                alamat
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                form_data["id_user"],
                form_data["nama_lengkap"],
                form_data["gender"],
                form_data["status_dalam_keluarga"],
                form_data["tempat_lahir"] or None,
                form_data["tanggal_lahir"] or None,
                form_data["agama"] or None,
                form_data["status_kepemilikan_rumah"],
                form_data["jumlah_anak"],
                form_data["alamat"] or None,
            ),
        )
        conn.commit()
        flash("Data warga berhasil ditambahkan.", "success")
        return redirect(url_for("warga_admin"))

    except Exception as e:
        print("CREATE WARGA ERROR:", e)
        flash(f"Gagal menambah data warga: {e}", "danger")
        return render_warga_form_page("Tambah Data Warga", "Simpan Data", form_data)
    finally:
        safe_close(cursor, conn)


@app.route("/warga-admin/edit/<int:id_warga>", methods=["GET", "POST"])
@admin_required
def edit_warga_admin(id_warga):
    conn = None
    cursor = None

    if request.method == "GET":
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT
                    id_warga,
                    id_user,
                    nama_lengkap,
                    gender,
                    status_dalam_keluarga,
                    tempat_lahir,
                    tanggal_lahir,
                    agama,
                    status_kepemilikan_rumah,
                    jumlah_anak,
                    alamat
                FROM warga
                WHERE id_warga = %s
                LIMIT 1
                """,
                (id_warga,),
            )
            data = cursor.fetchone()

            if not data:
                flash("Data warga tidak ditemukan.", "danger")
                return redirect(url_for("warga_admin"))

            form_data = {
                "id_user": data.get("id_user"),
                "nama_lengkap": data.get("nama_lengkap") or "",
                "gender": data.get("gender") or "Laki-laki",
                "status_dalam_keluarga": data.get("status_dalam_keluarga") or "lainnya",
                "tempat_lahir": data.get("tempat_lahir") or "",
                "tanggal_lahir": data["tanggal_lahir"].strftime("%Y-%m-%d") if data.get("tanggal_lahir") else "",
                "agama": data.get("agama") or "",
                "status_kepemilikan_rumah": data.get("status_kepemilikan_rumah") or "menumpang",
                "jumlah_anak": int(data.get("jumlah_anak", 0) or 0),
                "alamat": data.get("alamat") or "",
            }

            return render_warga_form_page("Edit Data Warga", "Update Data", form_data)

        except Exception as e:
            print("GET EDIT WARGA ERROR:", e)
            flash(f"Gagal memuat data edit warga: {e}", "danger")
            return redirect(url_for("warga_admin"))
        finally:
            safe_close(cursor, conn)

    form_data = get_warga_form_data(request.form)

    if not form_data["nama_lengkap"]:
        flash("Nama lengkap wajib diisi.", "danger")
        return render_warga_form_page("Edit Data Warga", "Update Data", form_data)

    if not form_data["agama"]:
        flash("Agama wajib dipilih.", "danger")
        return render_warga_form_page("Edit Data Warga", "Update Data", form_data)

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT id_warga FROM warga WHERE id_warga = %s LIMIT 1", (id_warga,))
        existing = cursor.fetchone()
        if not existing:
            flash("Data warga tidak ditemukan.", "danger")
            return redirect(url_for("warga_admin"))

        if form_data["id_user"] is not None:
            cursor.execute(
                "SELECT id_user FROM users WHERE id_user = %s LIMIT 1",
                (form_data["id_user"],),
            )
            user_row = cursor.fetchone()
            if not user_row:
                flash("User yang dipilih tidak ditemukan.", "danger")
                return render_warga_form_page("Edit Data Warga", "Update Data", form_data)

        cursor.close()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE warga
            SET id_user = %s,
                nama_lengkap = %s,
                gender = %s,
                status_dalam_keluarga = %s,
                tempat_lahir = %s,
                tanggal_lahir = %s,
                agama = %s,
                status_kepemilikan_rumah = %s,
                jumlah_anak = %s,
                alamat = %s
            WHERE id_warga = %s
            """,
            (
                form_data["id_user"],
                form_data["nama_lengkap"],
                form_data["gender"],
                form_data["status_dalam_keluarga"],
                form_data["tempat_lahir"] or None,
                form_data["tanggal_lahir"] or None,
                form_data["agama"] or None,
                form_data["status_kepemilikan_rumah"],
                form_data["jumlah_anak"],
                form_data["alamat"] or None,
                id_warga,
            ),
        )
        conn.commit()
        flash("Data warga berhasil diperbarui.", "success")
        return redirect(url_for("warga_admin"))

    except Exception as e:
        print("UPDATE WARGA ERROR:", e)
        flash(f"Gagal memperbarui data warga: {e}", "danger")
        return render_warga_form_page("Edit Data Warga", "Update Data", form_data)
    finally:
        safe_close(cursor, conn)


@app.route("/warga-admin/delete/<int:id_warga>", methods=["POST"])
@admin_required
def delete_warga_admin(id_warga):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM warga WHERE id_warga = %s",
            (id_warga,),
        )
        conn.commit()

        if cursor.rowcount < 1:
            flash("Data warga tidak ditemukan atau sudah terhapus.", "warning")
        else:
            flash("Data warga berhasil dihapus.", "success")

    except Exception as e:
        print("DELETE WARGA ERROR:", e)
        flash(f"Gagal menghapus data warga: {e}", "danger")
    finally:
        safe_close(cursor, conn)

    return redirect(url_for("warga_admin"))


@app.errorhandler(404)
def not_found(_error):
    if request.path == "/favicon.ico":
        return "", 204

    if request.path.startswith("/static/"):
        return "", 404

    if session.get("role") == "ketua_rt":
        return redirect(url_for("dashboard_admin"))
    if session.get("id_user"):
        return redirect(url_for("dashboard_warga"))
    return redirect(url_for("login"))


@app.errorhandler(500)
def internal_error(_error):
    flash("Terjadi kesalahan pada server.", "danger")
    if session.get("role") == "ketua_rt":
        return redirect(url_for("dashboard_admin"))
    if session.get("id_user"):
        return redirect(url_for("dashboard_warga"))
    return redirect(url_for("login"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)