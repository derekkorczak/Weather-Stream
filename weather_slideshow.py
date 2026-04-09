import os
import time
from io import BytesIO
import threading
import json
import hashlib
from datetime import datetime, timedelta
import subprocess
import sys
from flask import Flask, render_template, jsonify, request
import logging

import requests
from bs4 import BeautifulSoup
import pyodbc


def parse_expiration_datetime_input(raw):
    """
    Parse expiration from the 'd' prompt (YYYY-MM-DD HH:MM or with seconds).
    Strips whitespace and accepts ISO 'T' between date and time so we never
    build invalid strings like '... 15:30 :00' from trailing spaces.
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty expiration string")
    s = s.replace("T", " ", 1).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt, dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise ValueError(f"Invalid date/time format: {raw!r}")


def check_and_install_dependencies():
    """Check for required dependencies and install them if missing."""
    required_packages = {
        'requests': 'requests>=2.31.0',
        'PIL': 'Pillow>=10.0.0',
        'pyodbc': 'pyodbc>=5.0.1',
        'beautifulsoup4': 'beautifulsoup4>=4.12.0'
    }

    missing_packages = []

    # Check each required package
    for package_name, install_name in required_packages.items():
        try:
            if package_name == 'PIL':
                # PIL is imported as part of Pillow
                from PIL import Image
            elif package_name == 'beautifulsoup4':
                # beautifulsoup4 is imported as bs4
                __import__('bs4')
            else:
                __import__(package_name)
            print(f"[OK] {package_name} is already installed")
        except ImportError:
            missing_packages.append(install_name)
            print(f"[MISSING] {package_name} is missing")

    # Install missing packages
    if missing_packages:
        print(f"\nInstalling missing dependencies: {', '.join(missing_packages)}")
        print("This may take a few moments...\n")

        for package in missing_packages:
            try:
                print(f"Installing {package}...")
                # Use subprocess to run pip install
                result = subprocess.check_call([
                    sys.executable, '-m', 'pip', 'install', package
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                if result == 0:
                    print(f"[OK] Successfully installed {package}")
                else:
                    print(f"[FAILED] Failed to install {package}")

            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Error installing {package}: {e}")
                print("Please install manually using: pip install " + package)
            except Exception as e:
                print(f"[ERROR] Unexpected error installing {package}: {e}")

        print("\n" + "="*50)
        print("Please restart the application to use the newly installed packages.")
        print("="*50)
        input("Press Enter to exit...")
        sys.exit(1)

    # Note about tkinter
    try:
        import tkinter
        print("[OK] tkinter is available")
    except ImportError:
        print("[WARNING] tkinter is not available. This is usually included with Python.")
        print("  Please ensure you have a Python installation with tkinter support.")

    print("\nAll dependencies are ready!\n")

class WeatherSlideshowServer:
    def __init__(self):
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('weather_slideshow.log', mode='w')
            ]
        )

        # Disable verbose urllib3 connection logging
        logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)

        # Disable Flask/Werkzeug access logging
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
        logging.getLogger('flask.app').setLevel(logging.WARNING)

        logging.info("Weather Slideshow Server starting up")

        # Flask app
        self.app = Flask(__name__)

        # SQL Server connection details (from env in Docker/Portainer)
        self.SQL_SERVER = os.environ.get('SQL_SERVER', 'derekkorczak.bounceme.net,32787')
        self.SQL_DATABASE = os.environ.get('SQL_DATABASE', 'weather')
        self.SQL_USERNAME = os.environ.get('SQL_USERNAME', 'weather')
        self.SQL_PASSWORD = os.environ.get('SQL_PASSWORD', '')
        
        # Initialize database
        self.init_database()

        # Get display duration from user (for now, use default - could be made configurable later)
        self.display_duration = 30

        # Slideshow state (thread-safe)
        self.slideshow_state = {
            'current_image_index': 0,
            'countdown': self.display_duration,
            'display_duration': self.display_duration,
            'advancing': False  # True while finding next image (countdown stays 0)
        }
        self.expired_images = {}
        self.slide_durations = {}  # url -> seconds (custom duration per slide, default 30)
        self.state_lock = threading.Lock()
        self.expired_lock = threading.Lock()
        self.duration_lock = threading.Lock()

        # Background thread for slideshow timing
        self.slideshow_thread = None
        self.running = False

        # Setup Flask routes
        self.setup_routes()

    def get_browser_headers(self):
        """Return headers that mimic a real browser to avoid being blocked by weather.gov"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    def setup_routes(self):
        @self.app.route('/')
        def index():
            return render_template('index.html')

        @self.app.route('/api/current-image')
        def get_current_image():
            try:
                with self.state_lock:
                    current_index = self.slideshow_state['current_image_index']
                    countdown = self.slideshow_state['countdown']
                    display_duration = self.slideshow_state['display_duration']

                # Get the current image (slideshow_worker ensures it's not expired)
                url = self.image_urls[current_index]

                # Check expiration status for display - read directly from database to avoid caching issues
                expiration_text = ""
                try:
                    expired_data = self.get_image_expiration_from_db(url)
                    if expired_data:
                        expiration = expired_data.get('expiration')
                        if expiration:
                            expiration_dt = datetime.strptime(expiration, "%Y-%m-%d %H:%M:%S")
                            if datetime.now() <= expiration_dt:
                                expiration_text = f"Expires: {expiration_dt.strftime('%Y-%m-%d %H:%M')}"
                            else:
                                expiration_text = "Expired"
                        else:
                            expiration_text = "Manually Expired"
                except Exception as e:
                    logging.warning(f"Failed to read expiration from database for {url}: {e}")
                    # Fall back to cached data if database read fails
                    with self.expired_lock:
                        if url in self.expired_images:
                            expired_data = self.expired_images[url]
                            expiration = expired_data.get('expiration')
                            if expiration:
                                expiration_dt = datetime.strptime(expiration, "%Y-%m-%d %H:%M:%S")
                                if datetime.now() <= expiration_dt:
                                    expiration_text = f"Expires: {expiration_dt.strftime('%Y-%m-%d %H:%M')}"
                                else:
                                    expiration_text = "Expired"
                            else:
                                expiration_text = "Manually Expired"

                # Calculate when this image will change (current time + countdown seconds)
                next_change_timestamp = int(time.time()) + countdown

                # Get custom duration for display (only if different from default)
                custom_duration = None
                with self.duration_lock:
                    if url in self.slide_durations:
                        custom_duration = self.slide_durations[url]

                # URL encode the image URL for safe transmission
                from urllib.parse import quote
                encoded_url = quote(url, safe='')
                proxied_url = f'/api/image?url={encoded_url}'
                response = jsonify({
                    'image_url': proxied_url,
                    'countdown': countdown,
                    'expiration': expiration_text,
                    'next_change_timestamp': next_change_timestamp,
                    'display_duration': display_duration,
                    'custom_duration': custom_duration,
                    'image_index': current_index
                })
                # Prevent caching to ensure fresh data
                response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
                response.headers['Pragma'] = 'no-cache'
                response.headers['Expires'] = '0'
                return response

                logging.warning(f"Invalid image index {current_index}, returning empty response")
                return jsonify({
                    'image_url': '',
                    'countdown': countdown,
                    'expiration': '',
                    'next_change_timestamp': int(time.time()) + countdown,
                    'display_duration': display_duration,
                    'custom_duration': None
                })
            except Exception as e:
                logging.error(f"Error in get_current_image: {e}")
                return jsonify({'error': 'Internal server error'}), 500

        @self.app.route('/api/image')
        def proxy_image():
            try:
                # Get the URL from query parameters
                encoded_url = request.args.get('url')
                if not encoded_url:
                    logging.error("No URL parameter provided in image proxy request")
                    from flask import abort
                    abort(400)

                # URL decode the parameter
                from urllib.parse import unquote
                url = unquote(encoded_url)

                # Download and return the image with browser-like headers
                headers = self.get_browser_headers()

                response = requests.get(url, headers=headers, timeout=30)

                response.raise_for_status()

                # Return the image with appropriate headers
                from flask import Response
                content_type = response.headers.get('content-type', 'image/png')
                return Response(response.content, mimetype=content_type)
            except requests.exceptions.Timeout as e:
                logging.error(f"Timeout error proxying image {url}: {e}")
                from flask import abort
                abort(504)  # Gateway Timeout
            except requests.exceptions.ConnectionError as e:
                logging.error(f"Connection error proxying image {url}: {e}")
                from flask import abort
                abort(502)  # Bad Gateway
            except requests.exceptions.HTTPError as e:
                logging.error(f"HTTP error proxying image {url}: {e}")
                logging.error(f"Response status: {e.response.status_code if e.response else 'No response'}")
                logging.error(f"Response text: {e.response.text[:500] if e.response else 'No response text'}")
                from flask import abort
                abort(e.response.status_code if e.response else 500)
            except Exception as e:
                logging.error(f"Unexpected error proxying image {url}: {e}")
                logging.error(f"Exception type: {type(e).__name__}")
                from flask import abort
                abort(500)

        @self.app.route('/api/expire', methods=['POST'])
        def expire_image():
            try:
                with self.state_lock:
                    current_index = self.slideshow_state['current_image_index']

                if 0 <= current_index < len(self.image_urls):
                    url = self.image_urls[current_index]
                    logging.info(f"Expiring image at index {current_index}: {url}")
                    try:
                        headers = self.get_browser_headers()
                        response = requests.get(url, headers=headers, timeout=30)

                        if response.status_code == 200:
                            image_hash = self.get_image_hash(response.content)

                            # Store hash with no expiration (with expired lock)
                            with self.expired_lock:
                                self.expired_images[url] = {
                                    'hash': image_hash,
                                    'expiration': None
                                }
                                if not self.save_expired_images():
                                    return jsonify({'status': 'error', 'message': 'Failed to save expiration data'}), 500
                            logging.info(f"Successfully expired image: {url}")
                            return jsonify({'status': 'ok'})
                        else:
                            logging.warning(f"Failed to expire image - status {response.status_code}: {url}")
                            return jsonify({'status': 'error', 'message': f'Failed to fetch image: HTTP {response.status_code}'}), 400
                    except requests.exceptions.Timeout:
                        logging.error(f"Timeout expiring image {url}")
                        return jsonify({'status': 'error', 'message': 'Request timeout'}), 408
                    except requests.exceptions.ConnectionError:
                        logging.error(f"Connection error expiring image {url}")
                        return jsonify({'status': 'error', 'message': 'Connection error'}), 502
                    except Exception as e:
                        logging.error(f"Error expiring image {url}: {e}")
                        logging.error(f"Exception type: {type(e).__name__}")
                        return jsonify({'status': 'error', 'message': 'Internal error'}), 500
                else:
                    return jsonify({'status': 'error', 'message': 'Invalid image index'}), 400
            except Exception as e:
                logging.error(f"Unexpected error in expire_image: {e}")
                return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

        @self.app.route('/api/set-expiration', methods=['POST'])
        def set_expiration():
            try:
                data = request.get_json()
                expiration_date = data.get('expiration_date')

                if not expiration_date:
                    return jsonify({'status': 'error', 'message': 'No expiration date provided'}), 400

                with self.state_lock:
                    current_index = self.slideshow_state['current_image_index']

                if 0 <= current_index < len(self.image_urls):
                    url = self.image_urls[current_index]
                    try:
                        expiration_dt, expiration_str = parse_expiration_datetime_input(expiration_date)

                        # Validate the date is in the future
                        if expiration_dt <= datetime.now():
                            return jsonify({'status': 'error', 'message': 'Expiration date must be in the future'}), 400

                        # Get current image hash
                        logging.info(f"Setting expiration for image: {url}")
                        headers = self.get_browser_headers()
                        response = requests.get(url, headers=headers, timeout=30)

                        if response.status_code == 200:
                            image_hash = self.get_image_hash(response.content)
                            # Store both hash and expiration (with expired lock)
                            with self.expired_lock:
                                self.expired_images[url] = {
                                    'hash': image_hash,
                                    'expiration': expiration_str
                                }
                                if not self.save_expired_images():
                                    return jsonify({'status': 'error', 'message': 'Failed to save expiration data'}), 500
                            logging.info(f"Image will expire on: {expiration_str}")
                            return jsonify({'status': 'ok'})
                        else:
                            return jsonify({'status': 'error', 'message': f'Failed to fetch image: HTTP {response.status_code}'}), 400

                    except ValueError as e:
                        return jsonify({'status': 'error', 'message': f'Invalid date/time format: {e}'}), 400
                    except requests.exceptions.Timeout:
                        logging.error(f"Timeout setting expiration for image {url}")
                        return jsonify({'status': 'error', 'message': 'Request timeout'}), 408
                    except requests.exceptions.ConnectionError:
                        logging.error(f"Connection error setting expiration for image {url}")
                        return jsonify({'status': 'error', 'message': 'Connection error'}), 502
                    except Exception as e:
                        logging.error(f"Error setting expiration date: {e}")
                        return jsonify({'status': 'error', 'message': 'Internal error'}), 500
                else:
                    return jsonify({'status': 'error', 'message': 'Invalid image index'}), 400
            except Exception as e:
                logging.error(f"Unexpected error in set_expiration: {e}")
                return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

        @self.app.route('/api/set-duration', methods=['POST'])
        def set_duration():
            try:
                data = request.get_json()
                duration_seconds = data.get('duration_seconds')

                if duration_seconds is None:
                    return jsonify({'status': 'error', 'message': 'No duration provided'}), 400

                try:
                    duration_seconds = int(duration_seconds)
                except (TypeError, ValueError):
                    return jsonify({'status': 'error', 'message': 'Invalid duration'}), 400

                if duration_seconds < 1 or duration_seconds > 3600:
                    return jsonify({'status': 'error', 'message': 'Duration must be between 1 and 3600 seconds'}), 400

                with self.state_lock:
                    current_index = self.slideshow_state['current_image_index']

                if 0 <= current_index < len(self.image_urls):
                    url = self.image_urls[current_index]
                    with self.duration_lock:
                        self.slide_durations[url] = duration_seconds
                    with self.state_lock:
                        self.slideshow_state['display_duration'] = duration_seconds
                        self.slideshow_state['countdown'] = duration_seconds
                    logging.info(f"Set slide duration to {duration_seconds}s for: {url}")
                    return jsonify({'status': 'ok', 'duration_seconds': duration_seconds})
                else:
                    return jsonify({'status': 'error', 'message': 'Invalid image index'}), 400
            except Exception as e:
                logging.error(f"Unexpected error in set_duration: {e}")
                return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

        @self.app.route('/api/next', methods=['POST'])
        def next_image():
            try:
                with self.state_lock:
                    current_index = self.slideshow_state['current_image_index']
                    new_index = (current_index + 1) % len(self.image_urls)
                    new_url = self.image_urls[new_index]
                    duration = self.get_duration_for_url(new_url)
                    self.slideshow_state['current_image_index'] = new_index
                    self.slideshow_state['countdown'] = duration
                    self.slideshow_state['display_duration'] = duration

                logging.info(f"Manually advanced to next image at index {new_index}")
                return jsonify({'status': 'ok'})
            except Exception as e:
                logging.error(f"Error in next_image: {e}")
                return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

        @self.app.route('/api/legend')
        def get_legend():
            try:
                # Fetch legend data from weather.gov
                headers = self.get_browser_headers()
                response = requests.get('https://www.weather.gov/fgf/', headers=headers, timeout=30)
                response.raise_for_status()

                # Parse the HTML to extract legend entries
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find the legend container
                legend_container = soup.find('div', id='wfomap_rtcol_bot')
                legend_entries = []

                if legend_container:
                    # Find all legend entries
                    entries = legend_container.find_all('div', class_='wwamap-legend-entry')

                    for entry in entries:
                        # Extract the link text and color
                        link = entry.find('a')
                        if link:
                            text = link.get_text(strip=True)
                            # Extract the color swatch
                            color_div = entry.find('div', class_='wwamap-legend-color-swatch')
                            if color_div and 'style' in color_div.attrs:
                                # Extract background-color from style attribute
                                style = color_div['style']
                                if 'background-color:' in style:
                                    color = style.split('background-color:')[1].split(';')[0].strip()
                                    legend_entries.append({
                                        'text': text,
                                        'color': color
                                    })

                return jsonify({'legend_entries': legend_entries})

            except requests.exceptions.Timeout as e:
                logging.error(f"Timeout fetching legend data: {e}")
                return jsonify({'error': 'Request timeout'}), 408
            except requests.exceptions.ConnectionError as e:
                logging.error(f"Connection error fetching legend data: {e}")
                return jsonify({'error': 'Connection error'}), 502
            except Exception as e:
                logging.error(f"Error fetching legend data: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        # Image URLs
        self.image_urls = [
            "https://www.weather.gov/images/fgf/wxstory/Tab1FileL.png?08370a8a653e121a7a20f2bd6a2e93e2",
            "https://www.weather.gov/images/fgf/wxstory/Tab2FileL.png?08370a8a653e121a7a20f2bd6a2e93e2",
            "https://www.weather.gov/images/fgf/wxstory/Tab3FileL.png?08370a8a653e121a7a20f2bd6a2e93e2",
            "https://www.weather.gov/images/fgf/wxstory/Tab4FileL.png?08370a8a653e121a7a20f2bd6a2e93e2",
            "https://www.weather.gov/images/fgf/wxstory/Tab5FileL.png?08370a8a653e121a7a20f2bd6a2e93e2",
            "https://www.wpc.ncep.noaa.gov/noaa/noaad1.gif?1680885921",
            "https://www.spc.noaa.gov/products/outlook/day1otlk_1300.png",
            "https://radar.weather.gov/ridge/standard/KMVX_0.gif?refreshed=1680886770679",
            "https://www.weather.gov/wwamap/png/fgf.png",
            "https://graphical.weather.gov/images/fgf/MaxT1_fgf.png",
            "https://forecast.weather.gov/meteograms/Plotter.php?lat=47.915&lon=-97.0621&wfo=FGF&zcode=NDZ027&gset=18&gdiff=3&unit=0&tinfo=CY6&ahour=0&pcmd=10011111111000000000000000000000000000000000000000000000000&lg=en&indu=1!1!1!&dd=&bw=&hrspan=48&pqpfhr=6&psnwhr=6"
        ]

        # Load expired images
        with self.state_lock:
            self.expired_images = self.load_expired_images()
        
    def check_odbc_driver(self):
        try:
            import pyodbc
            drivers = pyodbc.drivers()
            if 'ODBC Driver 18 for SQL Server' not in drivers:
                print("\nODBC Driver 18 for SQL Server is not installed!")
                print("Please download and install it from:")
                print("https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
                print("\nAfter installation, please restart the application.")
                return False
            return True
        except Exception as e:
            print(f"Error checking ODBC drivers: {e}")
            return False

    def get_db_connection(self, max_retries=3, retry_delay=1.0):
        """Get a new database connection for thread-safe operations with retry logic"""
        conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={self.SQL_SERVER};DATABASE={self.SQL_DATABASE};UID={self.SQL_USERNAME};PWD={self.SQL_PASSWORD};TrustServerCertificate=yes;Connection Timeout=30'

        for attempt in range(max_retries):
            try:
                conn = pyodbc.connect(conn_str)
                return conn
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Failed to connect to database after {max_retries} attempts: {e}")
                    raise
                logging.warning(f"Database connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)

    def init_database(self):
        try:
            # Check for ODBC Driver first
            if not self.check_odbc_driver():
                logging.error("ODBC Driver 18 for SQL Server is not available. Exiting.")
                sys.exit(1)

            # Test connection and check if table exists
            conn = self.get_db_connection()
            try:
                cursor = conn.cursor()

                # Check if table exists
                cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ExpiredImages'")
                table_exists = cursor.fetchone()[0] > 0

                if not table_exists:
                    print("Warning: ExpiredImages table does not exist. Please create it with the following SQL:")
                    print('''
                    CREATE TABLE ExpiredImages (
                        url NVARCHAR(450) PRIMARY KEY,
                        image_hash NVARCHAR(450),
                        expiration_date DATETIME NULL
                    )
                    ''')
                    raise Exception("ExpiredImages table does not exist")

                print("Database initialized successfully")
            finally:
                conn.close()
        except Exception as e:
            print(f"Error initializing database: {e}")
            raise
            
    def get_image_expiration_from_db(self, url):
        """Get expiration data for a specific image URL directly from database"""
        for attempt in range(3):
            try:
                conn = self.get_db_connection()
                try:
                    cursor = conn.cursor()
                    cursor.execute('SELECT image_hash, expiration_date FROM ExpiredImages WHERE url = ?', (url,))
                    row = cursor.fetchone()
                    if row:
                        image_hash, expiration_date = row
                        return {
                            'hash': image_hash,
                            'expiration': expiration_date.strftime("%Y-%m-%d %H:%M:%S") if expiration_date else None
                        }
                    return None  # Image not in expired list
                finally:
                    conn.close()
            except Exception as e:
                if attempt == 2:
                    logging.warning(f"Failed to read expiration for {url} from database after 3 attempts: {e}")
                    return None
                logging.warning(f"Attempt {attempt + 1} to read expiration for {url} failed: {e}. Retrying...")
                time.sleep(0.5)

    def load_expired_images(self):
        for attempt in range(3):
            try:
                expired_images = {}
                conn = self.get_db_connection()
                try:
                    cursor = conn.cursor()
                    cursor.execute('SELECT url, image_hash, expiration_date FROM ExpiredImages')
                    for row in cursor.fetchall():
                        url, image_hash, expiration_date = row
                        expired_images[url] = {
                            'hash': image_hash,
                            'expiration': expiration_date.strftime("%Y-%m-%d %H:%M:%S") if expiration_date else None
                        }
                    return expired_images
                finally:
                    conn.close()
            except Exception as e:
                if attempt == 2:
                    logging.error(f"Failed to load expired images after 3 attempts: {e}")
                    return {}
                logging.warning(f"Attempt {attempt + 1} to load expired images failed: {e}. Retrying...")
                time.sleep(0.5)
            
    def save_expired_images(self):
        for attempt in range(3):
            try:
                conn = self.get_db_connection()
                try:
                    cursor = conn.cursor()
                    # Clear existing records
                    cursor.execute('DELETE FROM ExpiredImages')

                    # Insert new records
                    for url, data in self.expired_images.items():
                        expiration_date = None
                        if data.get('expiration'):
                            expiration_date = datetime.strptime(data['expiration'], "%Y-%m-%d %H:%M:%S")

                        cursor.execute('''
                            INSERT INTO ExpiredImages (url, image_hash, expiration_date)
                            VALUES (?, ?, ?)
                        ''', (url, data['hash'], expiration_date))

                    conn.commit()
                    return True
                finally:
                    conn.close()
            except Exception as e:
                if attempt == 2:
                    logging.error(f"Failed to save expired images after 3 attempts: {e}")
                    return False
                logging.warning(f"Attempt {attempt + 1} to save expired images failed: {e}. Retrying...")
                time.sleep(0.5)
            
    def get_image_hash(self, image_data):
        return hashlib.sha256(image_data).hexdigest()

    def get_duration_for_url(self, url):
        """Get effective display duration for a URL (custom or default)."""
        with self.duration_lock:
            return self.slide_durations.get(url, self.display_duration)
        
                
    def download_image(self, url):
        try:
            headers = self.get_browser_headers()

            response = requests.get(url, headers=headers, timeout=10)

            response.raise_for_status()

            # Check content type to ensure it's an image
            content_type = response.headers.get('content-type', '').lower()
            if not content_type.startswith('image/'):
                logging.warning(f"Invalid content type '{content_type}' for {url}")
                return None

            # Get current image hash for expiration checking
            current_hash = self.get_image_hash(response.content)

            # Check if image is in expired list (with expired lock)
            clear_duration_for_url = None
            with self.expired_lock:
                if url in self.expired_images:
                    expired_data = self.expired_images[url]
                    stored_hash = expired_data['hash']
                    expiration = expired_data.get('expiration')

                    # If hash has changed, remove from expired list and clear custom duration
                    if current_hash != stored_hash:
                        logging.info(f"New version of image found: {url}, removing from expired list")
                        del self.expired_images[url]
                        if not self.save_expired_images():
                            logging.error(f"Failed to save expired images after hash change for {url}")
                        clear_duration_for_url = url
                    # If no expiration date (manual expiration with 'e'), skip the image
                    elif expiration is None:
                        logging.info(f"Skipping manually expired image: {url}")
                        return None
                    # If has expiration date, check if it has passed
                    elif expiration:
                        try:
                            expiration_dt = datetime.strptime(expiration, "%Y-%m-%d %H:%M:%S")
                            now = datetime.now()
                            if now > expiration_dt:
                                logging.info(f"Image has passed expiration date: {expiration}")
                                logging.info(f"Skipping expired image: {url}")
                                return None
                            else:
                                logging.info(f"Image not yet expired, will expire on: {expiration}")
                        except ValueError as e:
                            logging.error(f"Invalid expiration date format '{expiration}' for {url}: {e}")
                            return None
                    else:
                        logging.warning(f"Unexpected expiration data for {url}: {expired_data}")

            # Clear custom duration when image updates (outside expired_lock to avoid deadlock)
            if clear_duration_for_url:
                with self.duration_lock:
                    self.slide_durations.pop(clear_duration_for_url, None)

            # Return a mock object to indicate success - we don't need the actual PIL image for slideshow validation
            return True

        except requests.exceptions.Timeout as e:
            logging.error(f"Timeout downloading image {url}: {e}")
            return None
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Connection error downloading image {url}: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error downloading image {url}: {e}")
            logging.error(f"Response status: {e.response.status_code if e.response else 'No response'}")
            if e.response:
                logging.error(f"Response text (first 500 chars): {e.response.text[:500]}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error downloading image {url}: {e}")
            logging.error(f"Exception type: {type(e).__name__}")
            return None
            
    # Removed resize_image method - images are handled by browser CSS for responsive display
    
    def start_slideshow_thread(self):
        """Start the background thread that manages slideshow timing"""
        if self.slideshow_thread and self.slideshow_thread.is_alive():
            return

        self.running = True
        self.slideshow_thread = threading.Thread(target=self.slideshow_worker, daemon=True)
        self.slideshow_thread.start()
        logging.info("Slideshow thread started")

    def stop_slideshow_thread(self):
        """Stop the background slideshow thread"""
        self.running = False
        if self.slideshow_thread:
            self.slideshow_thread.join(timeout=2)
            logging.info("Slideshow thread stopped")

    def slideshow_worker(self):
        """Background worker that handles slideshow timing and image rotation"""
        while self.running:
            time.sleep(1)  # Update every second

            with self.state_lock:
                countdown = self.slideshow_state['countdown']
                current_index = self.slideshow_state['current_image_index']
                advancing = self.slideshow_state['advancing']

                if advancing:
                    # Still finding next image; API returns countdown 0 until we're done
                    continue
                if countdown > 0:
                    self.slideshow_state['countdown'] = countdown - 1
                    continue

                # Countdown reached 0 - time to advance. Keep countdown at 0 until we have the next image.
                self.slideshow_state['advancing'] = True
                old_index = current_index
                next_index = (current_index + 1) % len(self.image_urls)
                max_attempts = len(self.image_urls)
                attempts = 0

            # Find the next non-expired image *outside* the lock so API stays responsive
            # (download_image can be slow over the network, especially when accessed remotely)
            while attempts < max_attempts:
                url = self.image_urls[next_index]
                image_valid = self.download_image(url)
                if image_valid:
                    break
                next_index = (next_index + 1) % len(self.image_urls)
                attempts += 1

            url = self.image_urls[next_index]
            duration = self.get_duration_for_url(url)
            with self.state_lock:
                self.slideshow_state['current_image_index'] = next_index
                self.slideshow_state['countdown'] = duration
                self.slideshow_state['display_duration'] = duration
                self.slideshow_state['advancing'] = False

            logging.info(f"Slideshow advanced from index {old_index} to {next_index} (skipped {attempts} expired images)")
        
    def run(self, host='0.0.0.0', port=8080, debug=False):
        """Start the Flask web server"""
        try:
            # Start the slideshow background thread
            self.start_slideshow_thread()

            print("\nWeather Slideshow Server")
            print(f"Access the slideshow at: http://localhost:{port}")
            print("Press Ctrl+C to stop the server")
            # Start Flask app
            self.app.run(host=host, port=port, debug=debug, threaded=True)

        except KeyboardInterrupt:
            print("\nShutting down server...")
        finally:
            self.stop_slideshow_thread()
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        # No persistent connection to clean up since we use per-request connections
        logging.info("Cleanup completed")

    def __del__(self):
        self.cleanup()

if __name__ == "__main__":
    if not os.environ.get('SKIP_DEPENDENCY_CHECK'):
        check_and_install_dependencies()
        import requests  # noqa: F401
        from PIL import Image  # noqa: F401 - ImageTk not used by server (would require tkinter)
        import pyodbc  # noqa: F401
    server = WeatherSlideshowServer()
    server.run() 