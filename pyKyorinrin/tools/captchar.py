import copy
import numpy as np
import cv2 as cv
import pyslibtesseract
import os

tesseract_config = pyslibtesseract.TesseractConfig(psm=pyslibtesseract.PageSegMode.PSM_SINGLE_CHAR)
tesseract_config.add_variable('tessedit_char_whitelist', 'QWERTYUIOPASDFGHJKLZXCVBNM')


def tse_read_captcha(file_name):
    img_start = cv.imread(file_name)
    height, width = img_start.shape[:2]

    # limpar topo da imagem
    img_start[0:15, 0:width] = np.full((15, width, 3), 255)

    img = copy.copy(img_start)

    ###
    # borrar, para tirar os ruídos
    img = cv.morphologyEx(img, cv.MORPH_CLOSE, np.ones((3, 3), np.uint8))

    ###
    # colocar imagem em tons de cinza e ignorar os pixels fracos de mais
    img = cv.cvtColor(img, cv.COLOR_BGR2GRAY)
    img = cv.cvtColor(img, cv.COLOR_GRAY2BGR)

    for i in img:
        for i2 in i:
            if (np.array([230, 230, 230]) >= i2).any():
                i2[...] = 0
            else:
                i2[...] = 255

    ###
    # preciso desgrudar as letras que estiverem colodas na borda
    img[height - 3:height, 0:width] = np.full((3, width, 3), 255)
    img[0:height, 0:3] = np.full((height, 3, 3), 255)
    img[0:height, width - 3:width] = np.full((height, 3, 3), 255)

    ###
    # apagar pontos pequenos, que podem atrapalhar o tesseract
    # partes grandes não é bom apagar, pois pode ser parte da letra essencial para o tesseract entender qual é
    # partes pequenas costumam mais atrapalhar que ajudar
    im2, contours, hierarchy = cv.findContours(cv.cvtColor(img, cv.COLOR_BGR2GRAY), cv.RETR_TREE, cv.CHAIN_APPROX_SIMPLE)

    letters = []
    added_x = []

    for i2 in contours:
        (x, y), radius = cv.minEnclosingCircle(i2)

        if radius <= 6:
            cv.drawContours(img, [i2], -1, (255, 255, 255), -1)

    ###
    # isolar cada letra
    img_circulada = copy.copy(img)
    for i2 in contours:
        (x, y), radius = cv.minEnclosingCircle(i2)

        if radius > 16 and radius < 30:
            center = (int(x), int(y))

            if int(x) in added_x:
                continue

            added_x.append(int(x))
            radius_int = int(radius + 15)

            x_min = x - radius_int
            y_min = y - radius_int
            if x_min < 0:
                x_min = 0
            if y_min < 0:
                y_min = 0
            letters.append((x, img[y_min : y + radius_int, x_min : x + radius_int]))
            cv.circle(img_circulada, center, radius_int + 1, (0,255,0), 1)

    letters_out = []
    if len(letters) > 0:
        # ler as letras isoladas
        loop = 0
        text = ''
        for i in letters:
            current_letter = i[1]

            # Salvar para tesseraczar
            cv.imwrite('letter' + str(loop) + '.png', current_letter)

            # Pegar o valor ASCII da letra
            new_char = rotate('letter' + str(loop) + '.png')[0]
            if len(new_char) and new_char[0] != ' ':
                text += new_char[0]
            else:
                text += '?'

            loop += 1
            letters_out.append((i[0], new_char[0]))

    letters_out = sorted(letters_out)
    letters_only = [i[1] for i in letters_out]
    return ''.join(letters_only)


def rotate(file_name):
    img = cv.imread(file_name)
    rows, cols = img.shape[:2]

    most_confidence = [' ', 0]
    again = False

    for i in range(-1, 2):
        M = cv.getRotationMatrix2D((cols/2,rows/2), 10 * i, 1)
        dst = cv.warpAffine(img,M,(cols,rows))

        letter_height, letter_width = dst.shape[:2]

        mask = np.zeros((letter_height + 2, letter_width + 2), np.uint8)
        mask[:] = 0
        for h in range(letter_height):
            cv.floodFill(dst, mask, (letter_width - 1, h), (255, 255, 255), upDiff=(200, 200, 200))
            cv.floodFill(dst, mask, (0, h), (255, 255, 255), upDiff=(200, 200, 200))

        for w in range(letter_width):
            cv.floodFill(dst, mask, (w, 0), (255, 255, 255), upDiff=(200, 200, 200))
            cv.floodFill(dst, mask, (w, letter_height - 1), (255, 255, 255), upDiff=(200, 200, 200))

        cv.imwrite(str(i) + file_name, dst)

        x = pyslibtesseract.LibTesseract.read_and_get_confidence_char(tesseract_config, str(i) + file_name)
        if len(x) == 0:
            continue
        new_char = x[0]
        if most_confidence[1] - 3 <= new_char[1] <= most_confidence[1] + 3:
            again = False
        else:
            again = True
        if new_char[0] != ' ' and most_confidence[1] < new_char[1]:
            most_confidence[0] = new_char[0]
            most_confidence[1] = new_char[1]

        os.remove(str(i) + file_name)

    if most_confidence[1] < 60 or again:
        for i in range(-5, 6):
            M = cv.getRotationMatrix2D((cols/2,rows/2), 10 * i, 1)
            dst = cv.warpAffine(img,M,(cols,rows))

            letter_height, letter_width = dst.shape[:2]

            mask = np.zeros((letter_height + 2, letter_width + 2), np.uint8)
            mask[:] = 0
            for h in range(letter_height):
                cv.floodFill(dst, mask, (letter_width - 1, h), (255, 255, 255), upDiff=(200, 200, 200))
                cv.floodFill(dst, mask, (0, h), (255, 255, 255), upDiff=(200, 200, 200))

            for w in range(letter_width):
                cv.floodFill(dst, mask, (w, 0), (255, 255, 255), upDiff=(200, 200, 200))
                cv.floodFill(dst, mask, (w, letter_height - 1), (255, 255, 255), upDiff=(200, 200, 200))

            cv.imwrite(str(i) + file_name, dst)

            x = pyslibtesseract.LibTesseract.read_and_get_confidence_char(tesseract_config, str(i) + file_name)
            if len(x) == 0:
                continue
            new_char = x[0]
            if new_char[0] != ' ' and most_confidence[1] < new_char[1]:
                most_confidence[0] = new_char[0]
                most_confidence[1] = new_char[1]

            os.remove(str(i) + file_name)

    os.remove(file_name)
    return most_confidence
