// Implementation of a KeyValueServer. Students should write their code in this file.
package p0

import (
    "net"
    "strconv"
    "bufio"
    "strings"
	"fmt"
)

//Estrutura servidor chave/valor
type keyValueServer struct {
    //Servidor TCP
    server *net.TCPListener
    //Canais
    // P/ enviar msgs a todos os clientes
    broadcastChannel chan []byte
    // P/ gerenciar msgs de put ao servidor
    queryPut chan commandPut
	// P/ gerenciar msgs de get ao servidor
    queryGet chan string
	//Canal que guarda a lista de clientes conectados
	connectedClients chan map[*net.TCPConn]*client
}

//Estrutura cliente
type client struct {
	//Conexão com o cliente
	connection       *net.TCPConn
	//Servidor
	server           *keyValueServer
	//Canais
	// P/ Receber entrada do cliente
	FromShellChannel chan []byte
	// P/ Escrever para o cliente
	ToShellChannel   chan []byte
}

//Estrutura Comando
//Para gerenciar msgs de put
type commandPut struct{
	//Chave
	key	string
	//Valor
	value []byte
}

//Cria e retorna um novo servidor
func New() KeyValueServer {
    //Cria um novo servidor
    newServer := &keyValueServer{
		//Inicia canais
    	connectedClients: make(chan map[*net.TCPConn]*client, 1),
		broadcastChannel: make(chan []byte),
		queryPut: make(chan commandPut),
		queryGet: make(chan string),
	}
	//Inicia o banco
	init_db()
	//Cria a lista de clientes
	newClientMap := make(map[*net.TCPConn]*client)
	//Coloca no canal
	newServer.connectedClients <- newClientMap
	//Retorna
    return newServer
}

//Inicia o servidor
func (kvs *keyValueServer) Start(port int) error {
    //Obtendo o endereço TCP, sobre o endereço localhost + porta passada.
    myAddress, addressError := net.ResolveTCPAddr("tcp","localhost:" +strconv.Itoa(port))
    //Teste se deu erro.
    if !(addressError == nil) {
        return addressError
    }
    //Iniciando o server a escutar no protocolo tcp, sobre o endereço obtido.
    server, serverError := net.ListenTCP("tcp", myAddress)
	//Teste se deu erro
	if !(serverError == nil){
		return serverError
	}
	//Atribui o server
    kvs.server = server

	//Inicia Go Routines
	// P/ Aceitar conexoes
    go kvs.acceptConnection()
    // P/ gerenciar o envio de msgs a todos
    go kvs.helperBroadcast()
	// P/ gerenciar o acesso ao banco
    go kvs.helperAccessPut()
    return nil
}

//Fecha o servidor e todos os clientes conectados
func (kvs *keyValueServer) Close() {
    //Fecha o servidor
    kvs.server.Close()
}

//Retorna o numero de usuarios conectados
func (kvs *keyValueServer) Count() int {
    //Obtem a lista de clientes
	clientList := <-kvs.connectedClients
	//Calcula o tamanho
	clientSize := len(clientList)
	//Devolve a lista de clientes
	kvs.connectedClients <- clientList
	//Retorna o numero de usuarios
	return clientSize
}

//Funcao para ficar aceitando conexoes
func (kvs *keyValueServer) acceptConnection(){
    for true{
        //Aceita uma conexao
        newClient, acceptError := kvs.server.AcceptTCP()
        //Testa erro
        if !(acceptError == nil){
            return
        }
        //Novo cliente
		clientAdd := &client{
			connection:       newClient,
			server:           kvs,
			FromShellChannel: make(chan []byte, 1),
			ToShellChannel:   make(chan []byte, 500),
		}
  		//Obtem a lista de clientes
        clientList := <- kvs.connectedClients
		//Adiciona o novo cliente
        clientList[clientAdd.connection] = clientAdd
        //Devolve a lista de clientes
		kvs.connectedClients <- clientList
        //Inicia Go Routines do cliente
        // P/ receber dados do cliente
        go clientAdd.helperFromShell()
        // P/ enviar dados ao cliente
        go clientAdd.helperToShell()
    }
}

//Funcao para gerenciar a entrada de um cliente
func (clie *client ) helperFromShell(){
    //Buffer de leitura
    buffReader := bufio.NewReader(clie.connection)
    for true{
        //Recebe uma mensagem, ate o /n
        menssage, menssageError := buffReader.ReadString('\n')
        //Testa erro
        if !(menssageError == nil) {
        	//Se der erro
        	//Obtem a lista
        	clientList := <- clie.server.connectedClients
        	//Remove o cliente
        	delete(clientList, clie.connection)
        	//Devolve a lista
        	clie.server.connectedClients <- clientList
        	//Fecha a conexao do clienteS
        	clie.connection.Close()
            return
        }
        //Separa a msg por virgula
		menssageArray := strings.Split(menssage, ",")
		//Envia ao canal certo, put ou get
		//Se o tamanho do array, entao é do tipo "put","chave","valor"
		//Comando put
		if len(menssageArray) == 3{
			//Cria novo comando
			newCommand := commandPut{
				key: menssageArray[1],
				value: []byte(strings.TrimSpace(menssageArray[2])),
			}
			//Poe no canal
			clie.server.queryPut <- newCommand
		//Se nao, entao é do tipo "get","chave"
		//Comando get
		}else{
			//Poe no canal
			clie.server.queryGet <- menssageArray[1]
		}
    }
}

//Funcao para gerenciar a escrita em um cliente
func (clie *client ) helperToShell(){
	for true{
		//Espera receber mensagens
		toWrite, writeError := <-clie.ToShellChannel
		//Testa erro
		if !writeError {
			return
		}
		//Escreve para o cliente
		_, err := clie.connection.Write(toWrite)
		//Testa erro
		if err != nil {
			return
		}
	}
}

//Funcao para gerenciar o envio de msgs a todos os clientes
func (kvs *keyValueServer) helperBroadcast(){
	 for true{
	 	//Espera msgs do canal
	 	toBroadcast, broadcastError := <-kvs.broadcastChannel
	 	//Testa erro
	 	if !broadcastError{
	 		return
		}
		//Obtem a lista de clientes
		clients := <- kvs.connectedClients
		//Para cada cliente
		for _, atual := range clients{
			//Se o buffer nao estiver cheio
			if len(atual.ToShellChannel) < 500{
				//Coloca a msg no canal do cliente
				atual.ToShellChannel <- toBroadcast
			}
		}
		//Devolve a lista de clientes
		kvs.connectedClients <- clients
	 }
}

//Funcao para gerenciar o acesso ao banco
func (kvs *keyValueServer) helperAccessPut(){
	for true {
		//Escolhe se há comando no canal de put ou de get
		select {
			//Se for put
			case newPut := <- kvs.queryPut:
				//Faz o comando de put
				put(newPut.key, newPut.value)
			//Se for get
			case newGet := <- kvs.queryGet:
				//Poe no canal de broadcast o resultado da operação de get
				kvs.broadcastChannel <- []byte(fmt.Sprintf("%v,%v\n", strings.TrimSpace(newGet), string(get(strings.TrimSpace(newGet)))))
		}
	}
}