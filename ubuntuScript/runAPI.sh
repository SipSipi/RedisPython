# This is script for terminal run either ubuntu and windows
flask run -h 10.2.1.64 -p 5000

$env:FLASK_APP = "sEDMSnPersonnelAPI.py"
flask run -h (host) -p (port)

$env:FLASK_APP = "personnelAPI.py"
flask run -h (host) -p (port)

$env:FLASK_APP = "edmsAsifAPI.py"
flask run -h (host) -p (port)

$env:FLASK_APP = "personnelAPI.py"
flask run -h (host) -p (port)