package consulfs

import (
	"errors"

	"github.com/bwester/consulfs/Godeps/_workspace/src/github.com/golang/glog"

	consul "github.com/bwester/consulfs/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/bwester/consulfs/Godeps/_workspace/src/golang.org/x/net/context"
)

// ConsulCanceler defines an API for accessing a Consul Key-Value store. It's mostly a
// clone of `github.com/hashicorp/consul/api.KV`, but it adds the ability to stop waiting
// for an operation if that operation has expired.
type ConsulCanceler interface {
	CAS(
		ctx context.Context,
		p *consul.KVPair,
		q *consul.WriteOptions,
	) (bool, *consul.WriteMeta, error)

	Delete(
		ctx context.Context,
		key string,
		w *consul.WriteOptions,
	) (*consul.WriteMeta, error)

	Get(
		ctx context.Context,
		key string,
		q *consul.QueryOptions,
	) (*consul.KVPair, *consul.QueryMeta, error)

	Keys(
		ctx context.Context,
		prefix string,
		separator string,
		q *consul.QueryOptions,
	) ([]string, *consul.QueryMeta, error)

	Put(
		ctx context.Context,
		p *consul.KVPair,
		q *consul.WriteOptions,
	) (*consul.WriteMeta, error)
}

// ErrCanceled is returned whenever a Consul operation is canceled.
var ErrCanceled = errors.New("operation canceled")

// CancelConsulKv is the concrete implementation of ConsulCanceler. It takes a Consul
// `Client` object and performs all operations using that client. When an operation is
// "canceled", the method call will return immediately with an ErrCanceled error
// returned. The underlying HTTP call is not aborted.
type CancelConsulKv struct {
	// The Consul client to use for executing operations.
	Client *consul.Client
}

// CAS performs a compare-and-swap on a key
func (cckv *CancelConsulKv) CAS(
	ctx context.Context,
	p *consul.KVPair,
	q *consul.WriteOptions,
) (bool, *consul.WriteMeta, error) {
	successCh := make(chan bool, 1)
	metaCh := make(chan *consul.WriteMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		success, meta, err := cckv.Client.KV().CAS(p, q)
		if glog.V(1) {
			glog.Infof("CAS(%s) -> %s, %s, %s", p, success, meta, err)
		}
		if err != nil {
			errCh <- err
		} else {
			successCh <- success
			metaCh <- meta
		}
	}()
	select {
	case success := <-successCh:
		meta := <-metaCh
		return success, meta, nil
	case err := <-errCh:
		return false, nil, err
	case <-ctx.Done():
		return false, nil, ErrCanceled
	}
}

// Delete removes a key and its data.
func (cckv *CancelConsulKv) Delete(
	ctx context.Context,
	key string,
	w *consul.WriteOptions,
) (*consul.WriteMeta, error) {
	metaCh := make(chan *consul.WriteMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		meta, err := cckv.Client.KV().Delete(key, w)
		if glog.V(1) {
			glog.Infof("Delete(%s, %s) -> %s, %s", key, w, meta, err)
		}
		if err != nil {
			errCh <- err
		} else {
			metaCh <- meta
		}
	}()
	select {
	case meta := <-metaCh:
		return meta, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ErrCanceled
	}
}

// Get returns the current value of a key.
func (cckv *CancelConsulKv) Get(
	ctx context.Context,
	key string,
	q *consul.QueryOptions,
) (*consul.KVPair, *consul.QueryMeta, error) {
	pairCh := make(chan *consul.KVPair, 1)
	metaCh := make(chan *consul.QueryMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		pair, meta, err := cckv.Client.KV().Get(key, q)
		if glog.V(1) {
			glog.Infof("Get(%s) -> %s, %s, %s", key, pair, meta, err)
		}
		if err != nil {
			errCh <- err
		} else {
			pairCh <- pair
			metaCh <- meta
		}
	}()
	select {
	case pair := <-pairCh:
		meta := <-metaCh
		return pair, meta, nil
	case err := <-errCh:
		return nil, nil, err
	case <-ctx.Done():
		return nil, nil, ErrCanceled
	}
}

// Keys lists all keys under a prefix
func (cckv *CancelConsulKv) Keys(
	ctx context.Context,
	prefix string,
	separator string,
	q *consul.QueryOptions,
) ([]string, *consul.QueryMeta, error) {
	keysCh := make(chan []string, 1)
	metaCh := make(chan *consul.QueryMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		keys, meta, err := cckv.Client.KV().Keys(prefix, separator, q)
		if glog.V(1) {
			glog.Infof("Keys(%s, %s) -> %s, %s, %s", prefix, separator, keys, meta, err)
		}
		if err != nil {
			errCh <- err
		} else {
			keysCh <- keys
			metaCh <- meta
		}
	}()
	select {
	case keys := <-keysCh:
		meta := <-metaCh
		return keys, meta, nil
	case err := <-errCh:
		return nil, nil, err
	case <-ctx.Done():
		return nil, nil, ErrCanceled
	}
}

// Put writes a key-value pair to the store
func (cckv *CancelConsulKv) Put(
	ctx context.Context,
	p *consul.KVPair,
	q *consul.WriteOptions,
) (*consul.WriteMeta, error) {
	metaCh := make(chan *consul.WriteMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		meta, err := cckv.Client.KV().Put(p, q)
		if glog.V(1) {
			glog.Infof("Put(%s, %s) -> %s, %s", p, q, meta, err)
		}
		if err != nil {
			errCh <- err
		} else {
			metaCh <- meta
		}
	}()
	select {
	case meta := <-metaCh:
		return meta, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ErrCanceled
	}
}
