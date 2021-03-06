import logging
import sqlite3

from flask import Flask, g, request, jsonify

app = Flask(__name__)

DATABASE = './stats.db'


def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value) for idx, value in enumerate(row))


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = make_dicts
    return db


def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


with app.app_context():
    query_db('CREATE TABLE IF NOT EXISTS agility'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0,'
             'marks BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS cooker'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS crafter'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS fisher'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0,'
             'profit BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS miner'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS runecrafter'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0,'
             'profit BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS smither'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS tanner'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'tanned BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS tithefarm'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0,'
             'points BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS wintertodt'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0,'
             'crates BIGINT(20) DEFAULT 0)')
    query_db('CREATE TABLE IF NOT EXISTS woodcutter'
             '(username TEXT PRIMARY KEY,'
             'runtime BIGINT(20) DEFAULT 0,'
             'experience BIGINT(20) DEFAULT 0)')
    get_db().commit()


@app.route('/api/stats/agility', methods=['PUT'])
def update_agility_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO agility (username, runtime, experience, marks)'
                 'VALUES (?, ?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?, marks = marks + ?',
                 [data['username'], data['runtime'], data['experience'], data['marks'],
                  data['runtime'], data['experience'], data['marks']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/cooker', methods=['PUT'])
def update_cooker_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO cooker (username, runtime, experience)'
                 'VALUES (?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?',
                 [data['username'], data['runtime'], data['experience'],
                  data['runtime'], data['experience']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/crafter', methods=['PUT'])
def update_crafter_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO crafter (username, runtime, experience)'
                 'VALUES (?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?',
                 [data['username'], data['runtime'], data['experience'],
                  data['runtime'], data['experience']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/fisher', methods=['PUT'])
def update_fisher_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO fisher (username, runtime, experience, profit)'
                 'VALUES (?, ?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?, profit = profit + ?',
                 [data['username'], data['runtime'], data['experience'], data['profit'],
                  data['runtime'], data['experience'], data['profit']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/miner', methods=['PUT'])
def update_miner_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO miner (username, runtime, experience)'
                 'VALUES (?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?',
                 [data['username'], data['runtime'], data['experience'],
                  data['runtime'], data['experience']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/runecrafter', methods=['PUT'])
def update_runecrafter_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO runecrafter (username, runtime, experience, profit)'
                 'VALUES (?, ?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?, profit = profit + ?',
                 [data['username'], data['runtime'], data['experience'], data['profit'],
                  data['runtime'], data['experience'], data['profit']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/smither', methods=['PUT'])
def update_smither_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO smither (username, runtime, experience)'
                 'VALUES (?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?',
                 [data['username'], data['runtime'], data['experience'],
                  data['runtime'], data['experience']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/tanner', methods=['PUT'])
def update_tanner_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO tanner (username, runtime, tanned)'
                 'VALUES (?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, tanned = tanned + ?',
                 [data['username'], data['runtime'], data['tanned'],
                  data['runtime'], data['tanned']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/tithefarm', methods=['PUT'])
def update_tithefarm_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO tithefarm (username, runtime, experience, points)'
                 'VALUES (?, ?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?, points = points + ?',
                 [data['username'], data['runtime'], data['experience'], data['points'],
                  data['runtime'], data['experience'], data['points']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/wintertodt', methods=['PUT'])
def update_wintertodt_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO wintertodt (username, runtime, experience, crates)'
                 'VALUES (?, ?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?, crates = crates + ?',
                 [data['username'], data['runtime'], data['experience'], data['crates'],
                  data['runtime'], data['experience'], data['crates']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/woodcutter', methods=['PUT'])
def update_woodcutter_stats():
    data = request.get_json()
    try:
        query_db('INSERT INTO woodcutter (username, runtime, experience)'
                 'VALUES (?, ?, ?) ON CONFLICT (username) DO UPDATE SET '
                 'runtime = runtime + ?, experience = experience + ?',
                 [data['username'], data['runtime'], data['experience'],
                  data['runtime'], data['experience']])
        get_db().commit()
        return '', 200
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/<string:script>', methods=['GET'])
def get_stats(script):
    try:
        return jsonify(query_db('SELECT * FROM ' + script))
    except sqlite3.OperationalError as e:
        return str(e), 500


@app.route('/api/stats/<string:script>/<string:username>', methods=['GET'])
def get_stats_username(script, username):
    try:
        return jsonify(query_db('SELECT * FROM ' + script + ' WHERE username = ?', [username]))
    except sqlite3.OperationalError as e:
        return str(e), 500


if __name__ == '__main__':
    logger = logging.getLogger('werkzeug')
    handler = logging.FileHandler('./log/flask.log')
    logger.addHandler(handler)
    app.logger.addHandler(handler)
    app.run(debug=True)
