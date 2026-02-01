import time
import os
import urllib.request
import random
import pydub
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
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
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading audio (attempt {attempt + 1}/{max_retries})...")
            urllib.request.urlretrieve(src, mp3_path)
            logger.info("✅ Audio file downloaded.")
            
            # Convert MP3 to WAV
            sound = pydub.AudioSegment.from_mp3(mp3_path)
            sound.export(wav_path, format="wav")
            logger.info("✅ Audio file converted to WAV.")
            return True
        except Exception as e:
            logger.error(f"❌ Audio download/conversion error (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return False

def find_and_solve_captcha(driver):
    """Main function to find and solve CAPTCHA"""
    try:
        # Switch to recaptcha iframe
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        recaptcha_frame = None
        
        for frame in frames:
            src = frame.get_attribute("src") or ""
            title = frame.get_attribute("title") or ""
            if "recaptcha" in src.lower() or "recaptcha" in title.lower():
                recaptcha_frame = frame
                break
        
        if recaptcha_frame:
            driver.switch_to.frame(recaptcha_frame)
            logger.info("Switched to recaptcha frame")
            
            # Try to find and click checkbox
            try:
                checkbox = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "recaptcha-checkbox-border"))
                )
                checkbox.click()
                logger.info("✅ Clicked reCAPTCHA checkbox")
                time.sleep(2)
                
                # Check if challenge appears
                driver.switch_to.default_content()
                return solve_audio_challenge(driver)
                
            except Exception as e:
                logger.error(f"❌ Error clicking checkbox: {e}")
                driver.switch_to.default_content()
                return "error"
        
        return "no_captcha"
        
    except Exception as e:
        logger.error(f"❌ Error in find_and_solve_captcha: {e}")
        driver.switch_to.default_content()
        return "error"

def solve_audio_challenge(driver):
    """Solve the audio challenge"""
    max_attempts = 5
    attempt = 0
    
    while attempt < max_attempts:
        attempt += 1
        logger.info(f"Audio challenge attempt {attempt}/{max_attempts}")
        
        # Find challenge iframe
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        challenge_frame = None
        
        for frame in frames:
            src = frame.get_attribute("src") or ""
            title = frame.get_attribute("title") or ""
            if "challenge" in src.lower() or "challenge" in title.lower():
                challenge_frame = frame
                break
        
        if not challenge_frame:
            logger.info("No challenge frame found - CAPTCHA might be already solved")
            return "solved"
        
        try:
            driver.switch_to.frame(challenge_frame)
            time.sleep(2)
            
            # Click audio challenge button
            try:
                audio_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "recaptcha-audio-button"))
                )
                audio_button.click()
                logger.info("✅ Clicked audio challenge button")
                time.sleep(2)
            except:
                # Try alternative selector
                try:
                    audio_button = driver.find_element(By.XPATH, "//button[contains(@title, 'audio')]")
                    audio_button.click()
                    logger.info("✅ Clicked audio challenge button (alternative selector)")
                    time.sleep(2)
                except Exception as e:
                    logger.error(f"❌ Could not find audio button: {e}")
                    driver.switch_to.default_content()
                    return "quit"
            
            # Get audio source
            try:
                audio_source = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "audio-source"))
                )
                src = audio_source.get_attribute("src")
                logger.info(f"📁 Audio source URL: {src[:100]}...")  # Log truncated URL
                
                if not src:
                    logger.error("❌ Audio source URL is empty")
                    continue
                
            except Exception as e:
                logger.error(f"❌ Could not find audio source: {e}")
                # Take screenshot for debugging
                try:
                    screenshot_path = f"captcha_error_{int(time.time())}.png"
                    driver.save_screenshot(screenshot_path)
                    logger.info(f"Screenshot saved: {screenshot_path}")
                except:
                    pass
                continue
            
            # Download and process audio
            mp3_path = os.path.join(os.getcwd(), f"captcha_audio_{attempt}.mp3")
            wav_path = os.path.join(os.getcwd(), f"captcha_audio_{attempt}.wav")
            
            if not download_audio_file(src, mp3_path, wav_path):
                continue
            
            # Recognize text
            captcha_text = voicereco(wav_path)
            
            if not captcha_text:
                logger.error("❌ Could not extract text from audio")
                continue
            
            # Enter the response
            try:
                response_box = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "audio-response"))
                )
                
                response_box.clear()
                time.sleep(0.5)
                
                # Type text character by character
                for ch in captcha_text.lower():
                    response_box.send_keys(ch)
                    time.sleep(random.uniform(0.05, 0.15))
                
                # Submit
                response_box.send_keys(Keys.ENTER)
                logger.info(f"✅ Submitted response: {captcha_text}")
                time.sleep(3)
                
                # Check if CAPTCHA is solved
                driver.switch_to.default_content()
                
                # Look for "Verify" button or similar
                try:
                    verify_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Verify')]")
                    verify_button.click()
                    logger.info("✅ Clicked Verify button")
                    time.sleep(2)
                except:
                    pass
                
                return "solved"
                
            except Exception as e:
                logger.error(f"❌ Error submitting response: {e}")
                continue
                
        except Exception as e:
            logger.error(f"❌ Error in audio challenge: {e}")
            driver.switch_to.default_content()
            continue
            
        finally:
            driver.switch_to.default_content()
    
    logger.error(f"❌ Failed to solve CAPTCHA after {max_attempts} attempts")
    return "quit"

def solve_recaptcha_audio(driver):
    """
    Main function to solve reCAPTCHA with better error handling
    """
    try:
        # First check if CAPTCHA is actually present
        time.sleep(2)
        
        # Look for any iframes with recaptcha
        result = find_and_solve_captcha(driver)
        
        if result == "solved":
            logger.info("🎉 CAPTCHA solved successfully!")
            return "solved"
        elif result == "no_captcha":
            logger.info("ℹ️ No CAPTCHA found")
            return "solved"
        else:
            logger.error("❌ Failed to solve CAPTCHA")
            return "quit"
            
    except Exception as e:
        logger.error(f"❌ Unexpected error in solve_recaptcha_audio: {e}")
        return "quit"