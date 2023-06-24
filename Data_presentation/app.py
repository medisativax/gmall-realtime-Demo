from flask import Flask
from flask import render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={
    r"/*": {
       "origins": "*"
    }
})

@app.route('/')
def hello_world():
    return render_template('index.html', name='index')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
