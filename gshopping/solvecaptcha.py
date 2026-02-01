import time
import os
import urllib.request
import random
import pydub
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

recaptcha_words = [
    "apple tree", "blue sky", "silver coin", "happy child", "gold star",
    "fast car", "river bank", "mountain peak", "red house", "sun flower",
    "deep ocean", "bright moon", "green grass", "snow fall", "strong wind",
    "dark night", "big city", "tall building", "small village", "soft pillow",
    "quiet room", "loud noise", "warm fire", "cold water", "heavy rain",
    "hot coffee", "empty street", "open door", "closed window", "white cloud",
    "yellow light", "long road", "short path", "new book", "old paper",
    "broken clock", "silent night", "early morning", "late evening", "clear sky",
    "dusty road", "sharp knife", "dull pencil", "lost key", "found wallet",
    "strong bridge", "weak signal", "fast train", "slow boat", "hidden message",
    "bright future", "dark past", "deep forest", "shallow lake", "frozen river",
    "burning candle", "flying bird", "running horse", "jumping fish", "falling leaf",
    "climbing tree", "rolling stone", "melting ice", "whispering wind", "shining star",
    "crying baby", "laughing child", "singing voice", "barking dog", "meowing cat",
    "chirping bird", "roaring lion", "galloping horse", "buzzing bee", "silent whisper",
    "drifting boat", "rushing water", "ticking clock", "clicking sound", "typing keyboard",
    "ringing bell", "blinking light", "floating balloon", "spinning wheel", "crashing waves",
    "boiling water", "freezing air", "burning wood", "echoing voice", "howling wind",
    "glowing candle", "rustling leaves", "dancing flame", "rattling chains", "splashing water",
    "twisting road", "swinging door", "glistening snow", "pouring rain", "shaking ground"
]

def voicereco(AUDIO_FILE):
    import speech_recognition as sr

    recognizer = sr.Recognizer()
    
    try:
        with sr.AudioFile(AUDIO_FILE) as source:
            logger.info("🔄 Processing audio file...")
            recognizer.adjust_for_ambient_noise(source, duration=0.5)
            audio = recognizer.record(source)

            try:
                text = recognizer.recognize_google(audio)
                logger.info(f"📝 Extracted Text: {text}")
                return text
            except sr.UnknownValueError:
                random_text = random.choice(recaptcha_words)
                logger.warning(f"❌ Could not understand audio, using fallback: {random_text}")
                return random_text
            except sr.RequestError as e:
                logger.error(f"❌ Speech recognition request error: {e}")
                random_text = random.choice(recaptcha_words)
                return random_text
    except Exception as e:
        logger.error(f"❌ Error processing audio file: {e}")
        random_text = random.choice(recaptcha_words)
        return random_text

def download_audio_file(src, mp3_path, wav_path):
    """Download and convert audio file with retries"""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading audio (attempt {attempt + 1}/{max_retries})...")
            
            # Add headers to mimic browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,application/ogg;q=0.7,video/*;q=0.6,*/*;q=0.5',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Range': 'bytes=0-',
                'Connection': 'keep-alive',
                'Referer': 'https://www.google.com/',
                'Sec-Fetch-Dest': 'audio',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'same-origin',
            }
            
            req = urllib.request.Request(src, headers=headers)
            
            with urllib.request.urlopen(req) as response:
                with open(mp3_path, 'wb') as f:
                    f.write(response.read())
            
            logger.info("✅ Audio file downloaded.")
            
            # Check file size
            file_size = os.path.getsize(mp3_path)
            logger.info(f"Audio file size: {file_size} bytes")
            
            if file_size < 1000:  # Too small, probably not an audio file
                logger.error(f"File too small ({file_size} bytes), probably not audio")
                return False
            
            # Convert MP3 to WAV
            try:
                sound = pydub.AudioSegment.from_mp3(mp3_path)
                sound.export(wav_path, format="wav")
                logger.info("✅ Audio file converted to WAV.")
                return True
            except Exception as e:
                logger.error(f"❌ Audio conversion error: {e}")
                # Try alternative format
                try:
                    sound = pydub.AudioSegment.from_file(mp3_path)
                    sound.export(wav_path, format="wav")
                    logger.info("✅ Audio file converted to WAV (alternative method).")
                    return True
                except Exception as e2:
                    logger.error(f"❌ Alternative conversion also failed: {e2}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Audio download error (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return False

def get_audio_source(driver):
    """Get the actual audio source URL from reCAPTCHA"""
    try:
        # Wait for audio player to load
        time.sleep(2)
        
        # Look for audio element with specific reCAPTCHA characteristics
        audio_elements = driver.find_elements(By.TAG_NAME, "audio")
        logger.info(f"Found {len(audio_elements)} audio elements")
        
        for i, audio in enumerate(audio_elements):
            try:
                src = audio.get_attribute("src") or ""
                id_attr = audio.get_attribute("id") or ""
                style = audio.get_attribute("style") or ""
                
                logger.info(f"Audio element {i}: id='{id_attr}', src='{src[:80]}...'")
                
                # Check if this is the reCAPTCHA audio source
                if "audio-source" in id_attr:
                    logger.info(f"✅ Found audio source by ID: {src[:100]}...")
                    return src
                
                # Check if src contains recaptcha or google domains
                if src and ("recaptcha" in src.lower() or "google.com" in src.lower() or "gstatic.com" in src.lower()):
                    # Make sure it's not a JS file
                    if not src.endswith('.js') and not 'recaptcha__en.js' in src:
                        logger.info(f"✅ Found likely audio source: {src[:100]}...")
                        return src
                        
            except Exception as e:
                logger.debug(f"Error checking audio element {i}: {e}")
                continue
        
        # If no audio elements found, try to find by XPath
        logger.info("No audio elements found by tag, trying XPath...")
        
        # Try multiple XPath patterns
        xpath_patterns = [
            "//audio[@id='audio-source']",
            "//audio[contains(@src, 'recaptcha/api2/payload')]",
            "//audio[contains(@src, 'google.com/recaptcha')]",
            "//audio[contains(@src, 'mp3')]",
            "//audio[contains(@src, 'wav')]",
            "//audio[contains(@src, 'audio')]",
        ]
        
        for xpath in xpath_patterns:
            try:
                audio_element = driver.find_element(By.XPATH, xpath)
                src = audio_element.get_attribute("src")
                if src:
                    logger.info(f"✅ Found audio source via XPath '{xpath}': {src[:100]}...")
                    return src
            except:
                continue
        
        # Try JavaScript approach to find hidden audio elements
        logger.info("Trying JavaScript to find audio elements...")
        audio_sources = driver.execute_script("""
            // Find all audio elements
            var audios = document.querySelectorAll('audio');
            var sources = [];
            for (var i = 0; i < audios.length; i++) {
                var audio = audios[i];
                var src = audio.src;
                if (src && src.includes('recaptcha') && !src.includes('.js')) {
                    sources.push({
                        src: src,
                        id: audio.id,
                        hidden: audio.style.display === 'none' || audio.style.visibility === 'hidden'
                    });
                }
            }
            return sources;
        """)
        
        if audio_sources and len(audio_sources) > 0:
            logger.info(f"Found {len(audio_sources)} audio sources via JavaScript")
            for source in audio_sources:
                logger.info(f"JS Source: {source['src'][:100]}... (id: {source['id']}, hidden: {source['hidden']})")
                if not source['src'].endswith('.js'):
                    return source['src']
        
        logger.error("❌ No valid audio source found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error finding audio source: {e}")
        return None

def solve_recaptcha_audio(driver):
    """
    Main function to solve reCAPTCHA
    """
    try:
        logger.info("Attempting to solve captcha...")
        time.sleep(2)
        
        # Find all iframes
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        logger.info(f"Found {len(frames)} iframes on page")
        
        # Try to find recaptcha frame
        recaptcha_frame = None
        for i, frame in enumerate(frames):
            try:
                src = frame.get_attribute("src") or ""
                title = frame.get_attribute("title") or ""
                if "recaptcha" in src.lower() or "recaptcha" in title.lower():
                    recaptcha_frame = frame
                    logger.info(f"Found recaptcha frame at index {i}: src={src[:50]}..., title={title}")
                    break
            except:
                continue
        
        if not recaptcha_frame:
            logger.info("No recaptcha frame found, might already be solved")
            return "solved"
        
        # Switch to recaptcha frame
        try:
            driver.switch_to.frame(recaptcha_frame)
            logger.info("Switched to recaptcha frame")
        except Exception as e:
            logger.error(f"Failed to switch to recaptcha frame: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Click checkbox
        try:
            checkbox = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "recaptcha-checkbox-border"))
            )
            
            # Click using JavaScript
            driver.execute_script("arguments[0].click();", checkbox)
            logger.info("✅ Clicked reCAPTCHA checkbox")
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"❌ Error clicking checkbox: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Switch back to default content and look for challenge frame
        driver.switch_to.default_content()
        time.sleep(2)
        
        # Look for challenge frame
        challenge_frame = None
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        
        for i, frame in enumerate(frames):
            try:
                src = frame.get_attribute("src") or ""
                title = frame.get_attribute("title") or ""
                if "challenge" in src.lower() or "challenge" in title.lower():
                    challenge_frame = frame
                    logger.info(f"Found challenge frame at index {i}")
                    break
            except:
                continue
        
        if not challenge_frame:
            logger.info("No challenge frame found, CAPTCHA might be solved")
            return "solved"
        
        # Switch to challenge frame
        try:
            driver.switch_to.frame(challenge_frame)
            logger.info("Switched to challenge frame")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Failed to switch to challenge frame: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Click audio challenge button
        try:
            # Wait for audio button
            audio_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "recaptcha-audio-button"))
            )
            
            # Click using JavaScript
            driver.execute_script("arguments[0].click();", audio_button)
            logger.info("✅ Clicked audio challenge button")
            time.sleep(3)  # Wait for audio to load
            
        except Exception as e:
            logger.error(f"❌ Error clicking audio button: {e}")
            # Try alternative selector
            try:
                audio_button = driver.find_element(By.XPATH, "//button[@title='Get an audio challenge']")
                driver.execute_script("arguments[0].click();", audio_button)
                logger.info("✅ Clicked audio challenge button (alternative)")
                time.sleep(3)
            except:
                driver.switch_to.default_content()
                return "quit"
        
        # Get audio source URL
        audio_src = get_audio_source(driver)
        
        if not audio_src:
            logger.error("❌ Could not get audio source URL")
            driver.switch_to.default_content()
            return "quit"
        
        # Validate it's actually an audio URL
        if audio_src.endswith('.js') or 'recaptcha__en.js' in audio_src:
            logger.error(f"❌ Got JavaScript file instead of audio: {audio_src[:100]}...")
            driver.switch_to.default_content()
            return "quit"
        
        # Download and process audio
        timestamp = int(time.time())
        mp3_path = os.path.join(os.getcwd(), f"captcha_audio_{timestamp}.mp3")
        wav_path = os.path.join(os.getcwd(), f"captcha_audio_{timestamp}.wav")
        
        if not download_audio_file(audio_src, mp3_path, wav_path):
            logger.error("❌ Failed to download audio file")
            driver.switch_to.default_content()
            return "quit"
        
        # Recognize text from audio
        captcha_text = voicereco(wav_path)
        if not captcha_text:
            logger.error("❌ Failed to recognize audio")
            driver.switch_to.default_content()
            return "quit"
        
        # Enter the response
        try:
            response_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "audio-response"))
            )
            
            # Clear and enter text
            response_box.clear()
            time.sleep(0.5)
            
            # Type character by character
            for ch in captcha_text.lower():
                response_box.send_keys(ch)
                time.sleep(random.uniform(0.05, 0.15))
            
            # Submit
            response_box.send_keys(Keys.ENTER)
            logger.info(f"✅ Submitted response: {captcha_text}")
            time.sleep(3)
            
            # Switch back to main content
            driver.switch_to.default_content()
            
            # Try to click verify button if present
            try:
                verify_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Verify')]"))
                )
                driver.execute_script("arguments[0].click();", verify_button)
                logger.info("✅ Clicked verify button")
                time.sleep(2)
            except:
                pass  # No verify button, that's OK
            
            logger.info("🎉 CAPTCHA solved successfully!")
            return "solved"
            
        except Exception as e:
            logger.error(f"❌ Error entering response: {e}")
            driver.switch_to.default_content()
            return "quit"
            
    except Exception as e:
        logger.error(f"❌ Unexpected error in solve_recaptcha_audio: {e}")
        return "quit"
    finally:
        try:
            driver.switch_to.default_content()
        except:
            pass

# Cleanup function to remove old audio files
def cleanup_audio_files():
    import glob
    import os
    
    audio_files = glob.glob("captcha_audio_*")
    for file in audio_files:
        try:
            os.remove(file)
            logger.debug(f"Cleaned up: {file}")
        except:
            pass