import os
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

def generate_zhuyin_pdf(pdf_path, result_data):
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    c = canvas.Canvas(pdf_path, pagesize=A4)
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
  
    font_cn_path = os.path.join(current_dir, "..", "fonts", "NotoSansTC-VariableFont_wght.ttf")
    pdfmetrics.registerFont(TTFont('CNStandard', font_cn_path))
    
    font_zy_path = os.path.join(current_dir, "..", "fonts", "NotoSansTC-VariableFont_wght.ttf")
    pdfmetrics.registerFont(TTFont('ZYStandard', font_zy_path))
    
    margin_left = 65 
    current_y = 750 
    line_height = 50 
    
    font_size_kanji = 26
    font_size_zy = 11 
    spacing_word = 8
    
    current_x = margin_left
    print(f"result_data: {result_data}")
    if not result_data or not result_data[0]:
        print("The result data is empty\n")
        return
        

    for item in result_data:
        char = item["char"]
        zy = item["zy"]
        
        if char == "\n":
            current_x = margin_left
            current_y -= line_height
            continue

        
        if zy != "":
            c.setFont('CNStandard', font_size_kanji)
            c.setFillColor(colors.black)
            c.drawString(current_x, current_y, char)
            kanji_width = c.stringWidth(char, 'CNStandard', font_size_kanji)
            
            zy_x = current_x + kanji_width + 3
            zy_y = current_y + (font_size_kanji * 0.1)
            c.setFont('ZYStandard', font_size_zy)
            c.setFillColor(colors.grey)
            c.drawString(zy_x, zy_y, zy)
            zy_width = c.stringWidth(zy, 'ZYStandard', font_size_zy)
            word_total_width = kanji_width + 3 + zy_width + spacing_word
            
        else:
            if char.isalpha() or char.isdigit():
                c.setFont('Helvetica', font_size_kanji)
                c.setFillColor(colors.black)
                c.drawString(current_x, current_y, char)
                word_total_width = c.stringWidth(char, 'Helvetica', font_size_kanji) + (spacing_word / 2)
            else:
                c.setFont('CNStandard', font_size_kanji)
                c.setFillColor(colors.black)
                c.drawString(current_x, current_y, char)
                word_total_width = c.stringWidth(char, 'CNStandard', font_size_kanji) + (spacing_word / 2)
                
        current_x += word_total_width
        if current_x > 530:
            current_x = margin_left
            current_y -= line_height
        if current_y < 60:
            c.showPage()
            current_y = 750
            current_x = margin_left
    c.showPage()
    c.save()
    print(f"The pdf file has been generated\n")
