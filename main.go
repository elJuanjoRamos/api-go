package main

import (
	"context"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
	"github.com/gorilla/mux"
	"strconv"
	"strings"
	"github.com/joho/godotenv"
	"cloud.google.com/go/pubsub"		
	"google.golang.org/api/option"
	"golang.org/x/oauth2/google"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
)

var MONGO_URI = "mongodb://sopes-cosmo-mongo:tWAzVW70NCpvAdz9k3ZybGAZGf6m8GMxD43zLXOJUPmKwUOhHBsg6XOQuWAiOgfmK2BvPh3Xzaykbadw4xfSyA==@sopes-cosmo-mongo.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@sopes-cosmo-mongo@"
var MYSQL_URI = "root:root@tcp(35.225.221.182)/Tweets"
var mysqlClient *sql.DB
var mongoClient *mongo.Client
var cant_tweets = 0
var actual_date = time.Now()

type Tweet struct {
	Comentario string `json:"comentario"`
	Downvotes  int    `json:"downvotes"`
	Fecha      string `json:"fecha"`
	Hashtags   []string `json:"hashtags"`
	Nombre     string `json:"nombre"`
	Upvotes    int `json:"upvotes"`
}

type Message struct {
	Cantidad 		int 
	Api 			string
	TiempoDeCarga 	string
}


func publicar(w http.ResponseWriter, r *http.Request) {
	//PARSE REQUEST TO STRUCT
	var tweet Tweet
	err := json.NewDecoder(r.Body).Decode(&tweet)
    if err != nil {
        fmt.Println(err.Error())
        return
    }

	//CONEXION A MYSQL
    mysqlClient, err := sql.Open("mysql", MYSQL_URI)
    if err != nil {
        log.Fatal(err)
    }

    pingErr := mysqlClient.Ping()
    if pingErr != nil {
        log.Fatal(pingErr)
    }
    fmt.Println("Connected!")

	//CONEXION A LA BASE DE DATOS E INSERCION DE DATOS
	mongoClient, err := mongo.NewClient(options.Client().ApplyURI(MONGO_URI))
	if err != nil {
		log.Fatal(err)
	}
	myContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = mongoClient.Connect(myContext)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to mongo db!")
	//defer cancel()
	defer mongoClient.Disconnect(myContext)


	////INSERTAR DATA MYSQL
	list := ""
	for index, element := range tweet.Hashtags {
		if index == 0 {
			list = element
		} else {
			list = list + "," + element
		}
	}

	newDate := strings.Split(tweet.Fecha, "/")
	actualDate := newDate[2] + "-" + newDate[1] + "-" + newDate[0]

	var query = "INSERT INTO Tweet(nombre, comentario, fecha, upvotes, downvotes, hashtags) VALUES(?,?,?,?,?,?)"
	_, err = mysqlClient.Exec(query, tweet.Nombre, tweet.Comentario, actualDate, strconv.Itoa(tweet.Upvotes), strconv.Itoa(tweet.Downvotes), list)

    if err != nil {
        panic(err.Error())
    }


	///INSERTAR EN MONGO
	collection := mongoClient.Database("Tweets").Collection("Tweet")
	_, insertErr := collection.InsertOne(myContext, tweet)
	if insertErr != nil {
		log.Fatal(insertErr)
	}
	defer cancel()

	//RESPUESTA
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	//w.Write(data)
	w.Write(makeResponse("Tweet saved"))

}

func iniciarCarga(w http.ResponseWriter, r *http.Request) {
	actual_date = time.Now()
	cant_tweets = 0;
	//RESPONSE
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(makeResponse("Conexion creada con exito"))

}

func finalizarCarga(w http.ResponseWriter, r *http.Request) {
	minute_init := parseDate(actual_date.Format("01-02-2006 15:04:05"))
    minute_finis := parseDate(time.Now().Format("01-02-2006 15:04:05"))

	err := publish((minute_finis - minute_init) *60)
	if err != nil {
		fmt.Println(err)
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(makeResponse("Conexion finalizada"))
}
func makeResponse(message string) []byte {
	resp := make(map[string]string)
	resp["message"] = message
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("Error happened in JSON marshal. Err: %s", err)
	}
	return jsonResp
}

func getEnvVar(key string) string {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error al cargar las variables de entorno")
	}
	return os.Getenv(key)
}

func publish(seconds int) error {
	PROJECT_ID := "sopes-326122"
	TOPIC_ID := "notificacion-carga"

	ctx := context.Background()


	//GET CREDENTIALS
	data, err := ioutil.ReadFile("./credentials.json")
    if err != nil {
        fmt.Println(err)
		return fmt.Errorf("Error: %v", err)
    }
	credentials, err := google.CredentialsFromJSON(ctx, []byte(data), pubsub.ScopePubSub)
    if err != nil {
        fmt.Println(err)
		return fmt.Errorf("Error: %v", err)
    }

	client, err := pubsub.NewClient(ctx, PROJECT_ID, option.WithCredentials(credentials))
	if err != nil {
		fmt.Println(err)
		fmt.Println("Error al crear el cliente Pub Sub")

	}
	// Topico al que se quiere enviar el mensaje
	t := client.Topic(TOPIC_ID)
	//Publicar Mensaje
	newMessage := &Message{
        Api:     		"go",
        Cantidad: 		cant_tweets,
        TiempoDeCarga: strconv.Itoa(seconds),
    }
	fmt.Println(newMessage)

	dataBytes, err := json.Marshal(newMessage)
    if err != nil {
        fmt.Println(err)
		return fmt.Errorf("Error: %v", err)
    }


	result := t.Publish(ctx, &pubsub.Message{Data: []byte(dataBytes)})
	//Bloquear contexto hasta que se tenga una respuesta
	id, err := result.Get(ctx)
	if err != nil {
		fmt.Println("Error al publicar un mensaje")
		return fmt.Errorf("Error: %v", err)
	}
	fmt.Println("Publicado un mensaje con ID:")
	fmt.Println(id)
	return nil
}

func parseDate(date string) int {
	dt := (strings.Split(date, " "))[1]
	actual_minute := (strings.Split(dt, ":"))[1]
	minute, _ := strconv.Atoi(actual_minute)
	return minute	
}



func main() {

	fmt.Println("Iniciando servidor ...")
	
	

	router := mux.NewRouter().StrictSlash(false)
	router.HandleFunc("/iniciarCarga", iniciarCarga).Methods("GET")
	router.HandleFunc("/publicar", publicar).Methods("POST")
	router.HandleFunc("/finalizarCarga", finalizarCarga).Methods("GET")

	if err := http.ListenAndServe(":2000", nil); err != nil {
		fmt.Println("Error al levantar el servidor")
	} else {
		fmt.Println("Servidor iniciado en el puerto 2000")
	}

}
