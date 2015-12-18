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
