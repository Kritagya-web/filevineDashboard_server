from flask import Flask, request, jsonify
import subprocess
import os

# Flask App
app = Flask(__name__)

# @app.route('/trigger-sync', methods=['POST'])
@app.route('/', methods=['POST'])
def trigger_sync():
    data = request.get_json()

    project_id = data.get("projectId")
    project_name = data.get("projectOrClientName", f"Project_{project_id}")

    if not project_id:
        return jsonify({"status": "error", "message": "Missing projectId"}), 400

    print("📥 Trigger Received:")
    print(f"🔹 Project ID   : {project_id}")
    print(f"🔹 Project Name : {project_name}")

    # Start your sync script with the projectId
    try:
        print(f"🚀 Calling sync script for project: {project_id} ({type(project_id)})")
        subprocess.Popen(["python", "filevine_to_zdrive_sync.py", str(project_id)], env=os.environ.copy())
        # subprocess.Popen(["python", "filevine_to_zdrive_sync.py", str(project_id)])
        return jsonify({
            "status": "success",
            "projectId": project_id,
            "projectName": project_name
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)

# from flask import Flask, request, jsonify

# app = Flask(__name__)

# @app.route("/", methods=["POST"])
# def sync_project():
#     data = request.get_json()
#     project_id = data.get("projectId")
#     client_name = data.get("projectOrClientName")
#     print(f"📥 Received Project ID: {project_id}, Client Name: {client_name}")
#     # Call your polling/sync logic here
#     return jsonify({"status": "success", "message": "Sync triggered"}), 200
