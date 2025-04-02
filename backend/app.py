import os

from flask import Flask, request, render_template, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename
from datetime import datetime
from script import process_csv
from flask_httpauth import HTTPBasicAuth
from flask import jsonify, make_response
from flask_cors import CORS  # You need to install this: pip install flask-cors
from flask import after_this_request


ALLOWED_EXTENSIONS = set(['xlsx', 'csv'])

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_app():
    app = Flask(__name__)
    CORS(app)  # This enables CORS for all routes


    @app.route('/process', methods=['GET', 'POST'])
    def process():
        if request.method == 'POST':
            print(request.files)
            file1 = request.files['artikel']
            # file1 = pd.read_excel(file1)
            print("Done reading articles")
            if file1 and allowed_file(file1.filename):
                filename = secure_filename(file1.filename)
                new_filename = f'{filename.split(".")[0]}_{str(datetime.now())}.xlsx'

                if not os.path.exists('input/file1'):
                    os.makedirs('input/file1')
                save_location1 = os.path.join('input/file1', new_filename)
                file1.save(save_location1)
                # file1.to_csv(save_location1)

            file2 = request.files['sales']
            # file2 = pd.read_excel(file2)
            print("Done reading sales")
            if file2 and allowed_file(file2.filename):
                filename = secure_filename(file2.filename)
                new_filename = f'{filename.split(".")[0]}_{str(datetime.now())}.xlsx'

                if not os.path.exists('input/file2'):
                    os.makedirs('input/file2')
                save_location2 = os.path.join('input/file2', new_filename)
                file2.save(save_location2)
                # file2.to_csv(save_location2)
            
            file3 = request.files['jira']
            # file3 = pd.read_excel(file3)
            print("Done reading jira")
            if file3 and allowed_file(file3.filename):
                filename = secure_filename(file3.filename)
                new_filename = f'{filename.split(".")[0]}_{str(datetime.now())}.csv'

                if not os.path.exists('input/file3'):
                    os.makedirs('input/file3')
                save_location3 = os.path.join('input/file3', new_filename)
                file3.save(save_location3)
                # file3.to_csv(save_location3)
            
            file4 = request.files['such']
            # file4 = pd.read_excel(file4)
            print("Done reading search")
            if file4 and allowed_file(file4.filename):
                filename = secure_filename(file4.filename)
                new_filename = f'{filename.split(".")[0]}_{str(datetime.now())}.xlsx'

                if not os.path.exists('input/file4'):
                    os.makedirs('input/file4')
                save_location4 = os.path.join('input/file4', new_filename)
                file4.save(save_location4)
                # file4.to_csv(save_location4)
            
            file5 = request.files['alt']
            # file5 = pd.read_excel(file5)
            print("Done reading search")
            if file5 and allowed_file(file5.filename):
                filename = secure_filename(file5.filename)
                new_filename = f'{filename.split(".")[0]}_{str(datetime.now())}.xlsx'

                if not os.path.exists('input/file5'):
                    os.makedirs('input/file5')
                save_location5 = os.path.join('input/file5', new_filename)
                file5.save(save_location5)
                # file5.to_csv(save_location4)

            output_file = process_csv(save_location1, save_location2, save_location3, save_location4, save_location5)
            delete_files_in_directory("input/file1")
            delete_files_in_directory("input/file2")
            delete_files_in_directory("input/file3")
            delete_files_in_directory("input/file4")
            delete_files_in_directory("input/file5")
            file_url = "/download/" + output_file


            data = {
                "message": "Files processed successfully",
                "result": {
                    # Any additional processing results you want to include
                },
                "fileUrl": file_url  # URL path where the file can be downloaded
            }

            return make_response(jsonify(data), 200)
            #return

        return render_template('upload.html')

    @app.route('/download')
    def download():
        return render_template('download.html', files=os.listdir('output'))

    @app.route('/download/<filename>')
    def download_file(filename):
        @after_this_request
        def remove_file(response):
            try:
                delete_files_in_directory("output/" + filename.split(".xlsx")[0])
                os.rmdir("output/" + filename.split(".xlsx")[0])
            except Exception as error:
                app.logger.error("Error removing or closing downloaded file handle", error)
            return response
        return send_from_directory('output/'+ filename.split(".xlsx")[0], filename)

    return app

def delete_files_in_directory(directory_path):
   try:
        files = os.listdir(directory_path)
        for file in files:
            file_path = os.path.join(directory_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        print("All files deleted successfully.")
   except OSError:
       print("Error occurred while deleting files.")

def page_not_found(e):
    app.logger.warning('User raised an 404: '  + str(e) + "/ "  + str(request.url))


if __name__ == "__main__":
    app = create_app()
    app.register_error_handler(404, page_not_found)
    port = int(os.environ.get('PORT', 8081))
    app.run(host='0.0.0.0', port=port)