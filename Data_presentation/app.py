from flask import Flask, render_template, request, redirect, url_for, session
from flask_cors import CORS
import pymysql

app = Flask(__name__)
app.secret_key = 'your_secret_key'
CORS(app, resources={
    r"/*": {
        "origins": "*"
    }
})

# 配置数据库连接
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'root'
app.config['MYSQL_DB'] = 'flask'
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

mysql = pymysql.connect(host=app.config['MYSQL_HOST'],
                        user=app.config['MYSQL_USER'],
                        password=app.config['MYSQL_PASSWORD'],
                        db=app.config['MYSQL_DB'],
                        cursorclass=pymysql.cursors.DictCursor)


# 假设有一个简单的用户数据库
# users = {'zhanglei': 'zhanglei'}

@app.route('/index')
def index():
    if 'username' in session:
        cursor = mysql.cursor()
        cursor.execute('SELECT count(*) from user')
        usercount = cursor.fetchone()['count(*)']
        return render_template('index.html', name='index',usercount=usercount)
    else:
        return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    # if request.method == 'POST':
    #     username = request.form['username']
    #     password = request.form['password']
    #     if username in users and users[username] == password:
    #         session['username'] = username
    #         return redirect(url_for('home'))
    # return render_template('login.html', name='login')
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        cursor = mysql.cursor()
        # print(f'SELECT * FROM user WHERE username={username} AND password={password}')    
        cursor.execute('SELECT * FROM user WHERE username=%s AND password=%s', (username, password))
        user = cursor.fetchone()
        if user:
            session['username'] = user['username']
            return redirect(url_for('home'))
    return render_template('login.html', name='login')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']
        if username != '' and password != '' and email != '':
            cursor = mysql.cursor()
            cursor.execute('SELECT * FROM user WHERE username=%s AND email=%s', (username, email))
            user = cursor.fetchone()
            if user:
                return redirect(url_for('register'))
            cursor = mysql.cursor()
            cursor.execute('INSERT INTO user (username, password, email) VALUES (%s, %s, %s)',
                               (username, password, email))
            mysql.commit()
            return redirect(url_for('login'))

    return render_template('register.html', name='register')


@app.route('/forgot', methods=['GET', 'POST'])
def forgot():
    if request.method == 'POST':
        username = request.form['username']
        new_password = request.form['password']
        email = request.form['email']
        cursor = mysql.cursor()
        cursor.execute('SELECT * FROM user WHERE username=%s AND email=%s', (username, email))
        user = cursor.fetchone()
        if user:
            cursor.execute('UPDATE user SET password=%s WHERE username=%s', (new_password, username))
            mysql.commit()
            return redirect(url_for('login'))

    return render_template('forgot_password.html', name='register')


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
