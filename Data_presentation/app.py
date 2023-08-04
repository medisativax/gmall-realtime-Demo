from flask import Flask, render_template, request, redirect, url_for, session
from flask_cors import CORS

app = Flask(__name__)
app.secret_key = 'your_secret_key'
CORS(app, resources={
    r"/*": {
        "origins": "*"
    }
})

# 假设有一个简单的用户数据库
users = {'zhanglei': 'zhanglei'}

@app.route('/index')
def index():
    if 'username' in session:
        return render_template('index.html', name='index')
    else:
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in users and users[username] == password:
            session['username'] = username
            return redirect(url_for('home'))
    return render_template('login.html', name='login')

@app.route('/')
@app.route('/home')
def home():
    if 'username' in session:
        return render_template('home_logined.html', name='home')
    return render_template('home.html', name='home')

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('home'))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
