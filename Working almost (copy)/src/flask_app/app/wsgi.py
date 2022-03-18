from main_app import flask_app

if __name__ == '__main__':
  #valid_de_dependencies() ###REMOVE THIS LINE IN PRODUCTION##### #use it in debug#
  flask_app.run('0.0.0.0', port=5001, debug=True)