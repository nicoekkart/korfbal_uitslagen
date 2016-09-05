from flask import *
import pandas as pd
app = Flask(__name__)

@app.route("/")
def show_tables():
    data = pd.read_csv('all_games.txt')
    titles = ['nummer', 'reeks', 'datum', 'thuisploeg', 'bezoekers',
              'aantal_thuis', 'aantal_bezoekers', 'scheidsrechter', 'jury', 'locatie']
    return render_template('main.html',tables=[
        data.to_html(na_rep='', justify='left', index=False, float_format=lambda x: '%.0f' % x)
    ])

if __name__ == "__main__":
    app.run(debug=True)