# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import signal
import threading
import time
import traceback
import random
import socket
import re

from sys import argv

from kafka import KafkaConsumer, KafkaProducer

HEADER = 10


class ManageMovimeinto(threading.Thread):
    def __init__(self, ip, port, alias, topic):
        self.ip_kafka = ip
        self.port_kakfa = port
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.mapa = []
        self.objetivo = [-1, -1]  # la casilla objetivo actual del visitante
        self.pos = [0, 0]  # posicion actual del visitante
        self.alias = alias
        self.topic = topic

    def stop(self):
        print("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
        self.stop_event.set()

    def objetivoAlcanzado(self, consumidor):
        aux = (self.objetivo[0], self.objetivo[1])
        tiempo = self.mapa[self.objetivo[0] * 20 + self.objetivo[1]]
        print("DISFRUTANDO DE LA ATRACCION: ", self.objetivo, "el tiempos: ", tiempo)
        if tiempo < 0:
            tiempo = 2
        time.sleep(tiempo)
        # self.esperaActiva(consumidor, tiempo)
        print("A BUSCAR NUEVA ATRACCION")

        atracciones = {}  # los indices on tuplas con su localizacion, los valores es el timepo de espera
        for i in range(0, len(self.mapa)):
            if (-1 < self.mapa[i] < 60) and ((i // 20, i % 20) != aux):
                atracciones[(i // 20, i % 20)] = self.mapa[i]
        try:
            self.objetivo = random.choice(list(atracciones))
        except Exception as e:
            print("ERROR al buscar nuvo objetivo. NO HAY NINGUN OBJETIVO DISPONIBLE, el usuairo data vueltas: ", e)
            self.objetivo = [random.randint(5, 15), random.randint(5, 15)]
        print("NUEVO OBJETIVO: ", self.objetivo)

    def eligeObjetivo(self):
        """
        A partir del mapa sacas las posiciones y los timepos de todas las atracciones
        :return:
        """
        try:
            if self.objetivo == [-1, -1] or self.mapa[self.objetivo[0] * 20 + self.objetivo[1]] >= 60:
                atracciones = {}  # los indices on tuplas con su localizacion, los valores es el timepo de espera
                for i in range(0, len(self.mapa)):
                    if -1 < self.mapa[i] < 60:
                        atracciones[(i // 20, i % 20)] = self.mapa[i]
                self.objetivo = random.choice(list(atracciones))
        except Exception as e:
            print("ERROR eligeObjetivo", e)

    def pintaMapa(self):
        print("aaaaaaf")
        for i in range(0, len(self.mapa)):
            if i % 20 == 0:
                print()

            if self.mapa[i] < -1:
                if i == self.pos[0] * 20 + self.pos[1]:
                    print('{:<4}'.format('#'), end="")
                else:
                    print('{:<4}'.format(chr(-(self.mapa[i]) + 66)), end="")
            elif self.mapa[i] == -1:
                print('{:<4}'.format('.'), end="")
            else:
                print('{:<4}'.format(self.mapa[i]), end="")

    def decideMovimiento(self, consumidor):
        if self.objetivo[0] == self.pos[0] and self.objetivo[1] == self.pos[1]:
            self.objetivoAlcanzado(consumidor)

        direc = ['NW', 'NN', 'NE', 'WW', 'EE', 'SW', 'SS', 'SE']
        direc_1 = []
        self.eligeObjetivo()
        # print("al inicio: ",self.pos)
        nuevas = []
        dist = []
        aux = -1
        for i in range(-1, 2):
            for j in range(-1, 2):
                if not (i == j and i == 0):
                    aux += 1
                    n = [self.pos[0] + i, self.pos[1] + j]
                    if not (n[0] < 0 or n[1] < 0):
                        d = ((self.objetivo[0] - n[0]) ** 2 + (self.objetivo[1] - n[1]) ** 2) ** (1 / 2)
                        nuevas.append(n)
                        dist.append(d)
                        direc_1.append(direc[aux])

        indice = dist.index(min(dist))
        self.pos = nuevas[indice]
        # print("al final: ",self.pos)
        # print("n: ",nuevas)
        # print("d: ",dist)
        # print("D:",((self.objetivo[0] - n[0])**2 + (self.objetivo[1] - n[1])**2)**(1/2))
        # self.pos = random.choice(nuevas)
        return str(self.pos[0] * 20 + self.pos[1])
        # return 'alyx:'+random.choice(['NN', 'SS', 'EE', 'WW'])

    def consumir(self, consumer, producer):
        global running
        print("CONSUMIENDO")
        time.sleep(3)
        producer.send(self.topic + 'in', str(0).encode())
        while (not self.stop_event.is_set()) and running:
            # print("aAAA")
            for msg in consumer:
                print("RRRRRRRRRRRRRRr: ", running, " ", consumer.subscription())
                if not running:
                    mens = b'no'
                    producer.send(self.topic + 'in', mens)
                    print("envio final: ", mens)
                    self.stop()
                    return
                else:
                    # print("recibido mapa: " + msg.value.decode())
                    # print("RECIBIDO: ", msg.value.decode())
                    self.mapa = eval(msg.value.decode())
                    print("posicion: ", self.pos, "objetivo: ", self.objetivo)
                    self.pintaMapa()
                    envio = self.decideMovimiento(consumer).encode()
                    print("enviando: ", envio)
                    producer.send(self.topic + 'in', envio)
                    time.sleep(1)

        producer.send(self.topic + 'in', b'no')

    def run(self):
        print("INICIO ManageMovimeinto")
        try:
            consumer = KafkaConsumer(bootstrap_servers=f'{self.ip_kafka}:{self.port_kakfa}',
                                     auto_offset_reset='earliest',
                                     consumer_timeout_ms=100)
            consumer.subscribe([self.topic + 'out'])
            producer = KafkaProducer(bootstrap_servers=f'{self.ip_kafka}:{self.port_kakfa}')
            self.consumir(consumer, producer)
            print("FINAL MOVIMIENTO")
            return
        except Exception as e:
            print("ERROR EN LectorSensores :", e)
            traceback.print_exc()
        finally:
            if 'consumer' in locals():
                consumer.close()
            print("FIN LectorMovimientos")

    def enAtraccion(self):
        loc = []
        for i in self.mapa:
            if i >= 0:
                loc.append(i)
        if (self.pos[0] * 20 + self.pos[1]) in loc:
            return True
        return False

    def esperaActiva(self, consumidor, tiempo):
        inicio = time.time()
        while True:
            for msg in consumidor:
                time.sleep(1)
                # self.mapa = eval(msg.value.decode())
                print("EN ESPERA:    -----    posicion: ", self.pos, "objetivo: ", self.objetivo)
                # self.pintaMapa()
                pass
                if time.time() - inicio > tiempo:
                    return


def signal_handler(sig, frame):
    """
    Maneja la flag de final para terminal el bucle infinito cuando se le manda SIGINT
    """
    global running
    print("TERMINANDO PROCESO DE VISITOR")
    running = False


def login():
    alias = input("alias: ")
    passwd = input("passwd: ")
    mensaje = (alias + '.' + passwd).encode()

    try:
        consumer = KafkaConsumer(bootstrap_servers=f'{ip_k}:{port_k}',
                                 auto_offset_reset='latest',
                                 consumer_timeout_ms=100)
        consumer.subscribe(['accesoout'])
        producer = KafkaProducer(bootstrap_servers=f'{ip_k}:{port_k}')
        print("envido: ", mensaje)
        producer.send('accesoin', mensaje)
        control = True
        while control:
            for msg in consumer:
                print(f"f: {msg.value}")
                if alias in msg.value.decode():
                    print("Login Exitoso: ", msg.value.decode())
                    a = ManageMovimeinto(ip_k, port_k, alias, msg.value.decode())
                    print("topic enviado: ", msg.value.decode())
                    a.start()
                    control = False
                else:
                    print("Credenciales incorrectas  /  aforo maximo ")
                    control = False

    except Exception as e:
        print("ERROR EN Login :", e)
        traceback.print_exc()
    finally:
        if 'consumer' in locals():
            consumer.close()


def comunicacion(cliente, mensaje) -> bool:
    ret = False
    length = str(len(mensaje)).encode()
    length_msg = length + b" " * (HEADER - len(length))
    print(f"enviando: {length_msg}")
    cliente.send(length_msg)
    print(f"enviando: {mensaje.encode()}")
    cliente.send(mensaje.encode())
    respuesta = cliente.recv(2)
    if respuesta == b'ok':
        ret = True
    elif respuesta == b'no':
        ret = False
    return ret


def registra_perfil(ip, port) -> bool:
    """
        :param port: int
        :param ip: string
        :return:
        """
    alias = input("alias: ")
    nombre = input("nombre: ")
    passwd = input("passwd: ")
    credentials = f"r:{alias}:{nombre}:{passwd}"
    ret = False
    try:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.connect((ip, port))
        ret = comunicacion(cliente, credentials)
        if ret:
            print("PERFIL REGISTRADO CON EXITO")
        else:
            print("NO SE HA PODIDO REGISTRAR, EL USUARIO YA EXISTE")
    except Exception as e:
        print("ERROR: ", e)
    finally:
        if 'cliente' in locals():
            cliente.close()

    print("CONEXION FINALIZADA")
    return ret


def actualiza_perfil(ip, port) -> bool:
    print("CREENCIALES ACTUALES")
    alias = input("alias: ")
    passwd = input("passwd: ")
    print("INTRODUCE LOS DATOS NUEVOS. # deja en blanco los datos qu eno queras cambiar")
    n_alias = input("alias: ")
    n_nombre = input("nombre: ")
    n_passwd = input("passwd: ")

    credentials = f"u:{alias}:{passwd}:{n_alias}:{n_nombre}:{n_passwd}"
    ret = False
    try:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.connect((ip, port))
        ret = comunicacion(cliente, credentials)
        if ret:
            print("PERFIL ACTUALIZADO CON EXITO")
        else:
            print("NO SE HA PODIDO ACTUALIZAR")
    except Exception as e:
        print("ERROR actualiza_perfil: ", e)
    finally:
        if 'cliente' in locals():
            cliente.close()

    print("CONEXION FINALIZADA actualiza_perfil")
    return ret


def pintaMenu():
    print("R - Registro: Registra n nuevo usuario")
    print("L - Login:  accede a la retransmision del mapa del parque")
    print("U - Update: modifica un ususairo registrado")
    print("Q - quit")


def filtra(args: list) -> bool:
    """
    Indica si el formato de los argumentos es el correcto
    :param args: Argumentos del programa
    """
    if len(args) != 3:
        print("Numero incorrecto de argumentos")
        return False

    regex_1 = '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{1,5}$'
    regex_2 = '^\S+:[0-9]{1,5}$'
    if not (re.match(regex_1, args[1]) or re.match(regex_2, args[1])):
        print("Direccion de servidor de registro incorrecta")
        return False

    if not (re.match(regex_1, args[2]) or re.match(regex_2, args[2])):
        print("Direccion de servidor kafka")
        return False

    return True


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if not filtra(argv):
        print("ERROR: Argumentos incorrectos")
        print("Usage: visitor.py <ip_registro:puerto> <ip_kafka:puerto> ")
        print("Example: visitor.py 192.168.56.33:4048 192.168.56.26:9092")
        exit()

    ip_r = argv[1].split(":")[0]
    port_r = int(argv[1].split(":")[1])
    ip_k = argv[2].split(":")[0]
    port_k = int(argv[2].split(":")[1])

    signal.signal(signal.SIGINT, signal_handler)
    running = True
    inn = ""
    while inn != "q":
        pintaMenu()
        inn = input("opcion> ")
        if inn == 'Q':
            exit()
        elif inn == 'R':
            registra_perfil(ip_r, port_r)
        elif inn == 'U':
            actualiza_perfil(ip_r, port_r)
        elif inn == 'L':
            login()
            # exit()
        else:
            print("OPCION NO VALIDA")
    print("FINAL")
    exit()
