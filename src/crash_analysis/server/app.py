import json
from flask import Flask, request

app = Flask(__name__)

@app.route("/crash", methods=["POST"])
def crash():
    '''
    Appends json body to a log file and print it to the stdout.
    '''
    body_dump = json.dumps(request.json)
    with open("crash.log", "a") as f:
        f.write(body_dump)
        f.write('\n')
    print(body_dump)
    return '', 204

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
