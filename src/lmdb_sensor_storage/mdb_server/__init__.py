# HTML template string without refresh
html_template = r"""<html><head>
<meta charset="utf-8" />
{head}
</head><body>
{body}
</body></html>
"""

# img = np.array((32, 32, 3), dtype=np.uint8)
# favicon = cv2.imencode('*.png', img)[1].tobytes()
favicon = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x03\x08\x00\x00\x00\x00w\xb6:^\x00\x00' \
          b'\x00\x0eIDAT\x08\x1dcP`P``\x06\x00\x01\t\x00D\x80E+\xa9\x00\x00\x00\x00IEND\xaeB`\x82'
