package NetSync

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"github.com/rokku-aaab/datastore"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/mono"
	"math/rand"
	"net"
)

type NetSync struct {
	network Network
	ds      *datastore.DataStore
}

type Network struct {
	Local string
	Pool  []string
}

type Update struct {
	ToDelete bool
	User     datastore.User
}

type Sync struct {
	BeginAt int
}

type Request struct {
	Type   int
	Update *Update
	Sync   *Sync
}

const (
	DefaultIP         = "tcp://0.0.0.0:4435"
	DefaultTLSIP      = "tcp://0.0.0.0:4455"
	RequestTypeUpdate = 1
	RequestTypeSync   = 2
)

func NewWithGlobalServers(localAddress string, store *datastore.DataStore) NetSync {
	ns := NetSync{ds: store, network: Network{
		Local: localAddress,
		Pool:  []string{""},
	}}

	ns.UpdateGlobalServerPool()

	return ns
}

func (ns NetSync) UpdateGlobalServerPool() {
	records, err := net.LookupTXT("rokku.aaab-online.xyz")

	if err == nil {
		ns.network.Pool = records
	}
}

func (ns NetSync) RunServer(tlsConfig *tls.Config) {
	_ = rsocket.Receive().
		Resume().
		Fragment(4*1024*1024). // Sync with a Fragment size of 4MB
		Acceptor(ns.acceptor).
		Transport(ns.network.Local).
		ServeTLS(context.Background(), tlsConfig)
}

func (ns NetSync) acceptor(setup payload.SetupPayload, socket rsocket.CloseableRSocket) (rsocket.RSocket, error) {
	return rsocket.NewAbstractSocket(
		rsocket.RequestResponse(func(msg payload.Payload) mono.Mono {
			elem, _ := Decode(msg.Data())
			request := elem.(Request)

			if request.Type == RequestTypeUpdate {
				update := request.Update

				if update.ToDelete {
					_ = ns.ds.Delete(update.User.UUID)
				} else {
					_ = ns.ds.Put(update.User)
				}
			}

			if request.Type == RequestTypeSync {
				sync := elem.(Sync)

				allUsers, _ := ns.ds.All()
				var users datastore.Users
				var buffer bytes.Buffer

				for i, user := range allUsers {
					if i < sync.BeginAt {
						continue
					}

					users = append(users, user)
				}

				buf, _ := Encode(users)
				buffer.Write(buf)

				return mono.Just(payload.New(buffer.Bytes(), []byte{}))
			}
			return mono.Just(payload.New([]byte{}, []byte{}))
		}),
	), nil
}

func (ns NetSync) SendUpdate(update Update) error {
	pload, err := Encode(update)

	if err != nil {
		return err
	}

	var client rsocket.Client
	for _, ip := range ns.network.Pool {
		client, err = rsocket.Connect().
			Resume().
			Fragment(1024).
			SetupPayload(payload.New([]byte{}, []byte{})).
			Transport(ip).
			Start(context.Background())

		if client == nil {
			return nil
		}

		client.FireAndForget(payload.New(pload, []byte{}))

		client.Close()
		client = nil

		if err != nil {
			return err
		}
	}

	return nil
}

func (ns NetSync) RequestSync(begin int) error {
	pload, err := Encode(Sync{BeginAt: begin})

	var client rsocket.Client

	ri := rand.Intn(len(ns.network.Pool))
	ip := ns.network.Pool[ri]

	client, err = rsocket.Connect().
		Resume().
		Fragment(1024).
		SetupPayload(payload.New([]byte{}, []byte{})).
		Transport(ip).
		Start(context.Background())

	if client == nil {
		return nil
	}

	resp := client.RequestResponse(payload.New(pload, []byte{}))

	resp.DoOnSuccess(func(data payload.Payload) {
		buffer := data.Data()

		elems, _ := Decode(buffer)
		users := elems.(datastore.Users)

		for _, user := range users {
			_ = ns.ds.Put(user)
		}
	})

	client.Close()
	client = nil

	if err != nil {
		return err
	}

	return nil
}

func Encode(elem interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)

	err := enc.Encode(elem)

	if err != nil {
		return []byte{}, err
	}

	return buffer.Bytes(), nil
}

func Decode(byts []byte) (interface{}, error) {
	var buffer bytes.Buffer
	_, err1 := buffer.Write(byts)

	if err1 != nil {
		return nil, err1
	}

	dec := gob.NewDecoder(&buffer)

	var elem interface{}
	err := dec.Decode(&elem)

	if err != nil {
		return nil, err
	}

	return elem, nil
}
