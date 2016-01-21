from PIL import Image
import os


def element_image_download(phantom, element, padding_x=0, padding_y=0, padding_width=0, padding_height=0, file_name='element'):
    phantom.save_screenshot('temp.jpg')
    im = Image.open('temp.jpg')

    location = element.location
    size = element.size

    location['x'] += padding_x
    location['y'] += padding_y
    size['width'] += padding_width
    size['height'] += padding_height
    box = (location['x'], location['y'], size['width'] + location['x'], size['height'] + location['y'])

    region = im.crop(box)
    region.save(file_name + '.jpg', 'JPEG')

    os.remove('temp.jpg')

def alert_work(phantom):
    # Gambiarra para poder ler as mensagens em de javascript "alert" nas páginas, pois isso não foi implementado no GhostDriver
    # https://github.com/detro/ghostdriver/issues/20
    # Irá adicionar ao phantom o método last_alert_message(), da qual retorna o texto do último alert
    phantom.execute_script("""
    window.alert.last_alert_message = undefined;
    window.alert = function(message) {
        window.alert.last_alert_message = message;
    }
    """)
    phantom.last_alert_message = lambda: phantom.execute_script('return window.alert.last_alert_message')
