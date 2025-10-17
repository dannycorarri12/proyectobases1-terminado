from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from db import Database
import logging

app = Flask(__name__, static_folder='public', static_url_path='')
CORS(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"  
NEO4J_DATABASE = "proyectobases"               

try:
    db = Database(
        uri=NEO4J_URI, 
        user=NEO4J_USER, 
        password=NEO4J_PASSWORD,
        db_name=NEO4J_DATABASE
    )
    logging.info(f"Conexión con Neo4j (base de datos '{NEO4J_DATABASE}') establecida exitosamente.")
except Exception as e:
    logging.error(f"No se pudo conectar a Neo4j: {e}")
    db = None

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'interfaz.html')

ENTITY_MAP = {
    "personas": "persona",
    "libros": "libro",
    "autores": "autor",
    "clubes": "club"
}

@app.route('/<plural_entity>', methods=['GET', 'POST'])
def handle_entities_plural(plural_entity):
    entity = ENTITY_MAP.get(plural_entity.lower())
    if not entity:
        return jsonify({"error": f"La entidad '{plural_entity}' no es válida."}), 404
    
    if request.method == 'GET':
        return get_entities(entity)
    elif request.method == 'POST':
        return add_entity(entity)

@app.route('/<plural_entity>/<path:identifier>', methods=['PUT'])
def handle_entity_singular(plural_entity, identifier):
    entity = ENTITY_MAP.get(plural_entity.lower())
    if not entity:
        return jsonify({"error": f"La entidad '{plural_entity}' no es válida."}), 404

    if request.method == 'PUT':
        return update_entity(entity, identifier)

def get_entities(entity):
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    try:
        nodes = db.get_all_nodes(entity)
        return jsonify(nodes)
    except Exception as e:
        logging.error(f"Error al obtener entidades '{entity}': {e}")
        return jsonify({"error": f"Error interno al obtener {entity}s"}), 500

def add_entity(entity):
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    data = request.json
    if not data: return jsonify({"error": "No se proporcionaron datos."}), 400
    try:
        db.add_node(entity, data)
        return jsonify({"message": f"{entity.capitalize()} agregado correctamente."}), 201
    except Exception as e:
        logging.error(f"Error al agregar entidad '{entity}': {e}")
        return jsonify({"error": f"Error interno al agregar {entity}"}), 500

def update_entity(entity, identifier):
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    data = request.json
    if not data: return jsonify({"error": "No se proporcionaron datos para actualizar."}), 400
    try:
        db.update_node(entity, identifier, data)
        return jsonify({"message": f"{entity.capitalize()} actualizado correctamente."})
    except Exception as e:
        logging.error(f"Error al actualizar {entity} '{identifier}': {e}")
        return jsonify({"error": f"Error interno al actualizar {entity}"}), 500

@app.route('/relaciones/<tipo_relacion>', methods=['POST'])
def crear_relacion(tipo_relacion):
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    data = request.json
    from_node = data.get('from')
    to_nodes = data.get('to')
    if not from_node or not to_nodes:
        return jsonify({"error": "Datos incompletos para crear la relación."}), 400
    try:
        db.crear_relaciones(tipo_relacion, from_node, to_nodes)
        return jsonify({"message": "Relaciones creadas exitosamente."}), 201
    except Exception as e:
        logging.error(f"Error al crear relación '{tipo_relacion}': {e}")
        return jsonify({"error": "Error interno al crear las relaciones."}), 500

@app.route('/consultas/libros-leidos', methods=['GET'])
def get_libros_leidos():
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    persona_nombre = request.args.get('persona')
    if not persona_nombre: return jsonify({"error": "El nombre de la persona es requerido."}), 400
    try:
        resultado = db.consulta_libros_leidos(persona_nombre)
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'libros leidos': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/consultas/personas-club', methods=['GET'])
def get_personas_club():
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    club_nombre = request.args.get('club')
    if not club_nombre: return jsonify({"error": "El nombre del club es requerido."}), 400
    try:
        resultado = db.consulta_personas_club(club_nombre)
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'personas club': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/consultas/personas-mas-libros', methods=['GET'])
def get_personas_mas_libros():
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    try:
        resultado = db.consulta_personas_mas_libros()
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'personas mas libros': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/consultas/personas-mas-clubes', methods=['GET'])
def get_personas_mas_clubes():
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    try:
        resultado = db.consulta_personas_mas_clubes()
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'personas mas clubes': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/consultas/libros-populares', methods=['GET'])
def get_libros_populares():
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    try:
        resultado = db.consulta_libros_populares()
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'libros populares': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/admin/cargar-datos', methods=['POST'])
def cargar_datos_iniciales():
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    try:
        mensaje = db.cargar_datos_iniciales()
        return jsonify({"message": mensaje}), 200
    except Exception as e:
        logging.error(f"Error en la carga inicial de datos: {e}")
        return jsonify({"error": f"Error masivo al cargar datos: {e}"}), 500

@app.route('/admin/cargar-datos-manual', methods=['POST'])
def cargar_datos_manuales():
    if not db: return jsonify({"error": "La base de datos no está disponible."}), 500
    if 'csv_files' not in request.files:
        return jsonify({"error": "No se encontraron archivos en la petición."}), 400
    
    files = request.files.getlist('csv_files')
    if not files:
        return jsonify({"error": "No se seleccionó ningún archivo."}), 400

    file_contents = {}
    for file in files:
        if file.filename == '': continue
        try:
            content = file.stream.read().decode("utf-8-sig")
            file_contents[file.filename] = content
        except Exception as e:
            logging.error(f"Error leyendo el archivo {file.filename}: {e}")
            return jsonify({"error": f"No se pudo leer el archivo {file.filename}"}), 500
            
    try:
        mensaje = db.cargar_datos_manualmente(file_contents)
        return jsonify({"message": mensaje}), 200
    except Exception as e:
        logging.error(f"Error en la carga manual de datos: {e}")
        return jsonify({"error": f"Error masivo al procesar archivos: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True)

