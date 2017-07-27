package consulfs

import (
	"github.com/Sirupsen/logrus"
	consul "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
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

// CancelConsulKV is the concrete implementation of ConsulCanceler. It takes a Consul
// `Client` object and performs all operations using that client. When an operation is
// "canceled", the method call will return immediately with the context error. The
// underlying HTTP call is not aborted.
type CancelConsulKV struct {
	// The Consul client to use for executing operations.
	Client *consul.Client
	// Logger gets all the logging messages
	Logger *logrus.Logger
}

// CAS performs a compare-and-swap on a key
func (cckv *CancelConsulKV) CAS(
	ctx context.Context,
	p *consul.KVPair,
	q *consul.WriteOptions,
) (bool, *consul.WriteMeta, error) {
	successCh := make(chan bool, 1)
	metaCh := make(chan *consul.WriteMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		cckv.Logger.WithField("key", p.Key).Debug(" => CAS")
		success, meta, err := cckv.Client.KV().CAS(p, q)
		cckv.Logger.WithFields(logrus.Fields{
			"key":           p.Key,
			"kv":            p,
			"success":       success,
			"meta":          meta,
			logrus.ErrorKey: err,
		}).Debug(" <= CAS")
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
		return false, nil, ctx.Err()
	}
}

// Delete removes a key and its data.
func (cckv *CancelConsulKV) Delete(
	ctx context.Context,
	key string,
	w *consul.WriteOptions,
) (*consul.WriteMeta, error) {
	metaCh := make(chan *consul.WriteMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		cckv.Logger.WithField("key", key).Debug(" => Delete")
		meta, err := cckv.Client.KV().Delete(key, w)
		cckv.Logger.WithFields(logrus.Fields{
			"key":           key,
			"options":       w,
			"meta":          meta,
			logrus.ErrorKey: err,
		}).Debug(" <= Delete")
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
		return nil, ctx.Err()
	}
}

// Get returns the current value of a key.
func (cckv *CancelConsulKV) Get(
	ctx context.Context,
	key string,
	q *consul.QueryOptions,
) (*consul.KVPair, *consul.QueryMeta, error) {
	pairCh := make(chan *consul.KVPair, 1)
	metaCh := make(chan *consul.QueryMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		cckv.Logger.WithField("key", key).Debug(" => Get")
		pair, meta, err := cckv.Client.KV().Get(key, q)
		cckv.Logger.WithFields(logrus.Fields{
			"key":           key,
			"options":       q,
			"kv":            pair,
			"meta":          meta,
			logrus.ErrorKey: err,
		}).Debug(" <= Get")
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
		return nil, nil, ctx.Err()
	}
}

// Keys lists all keys under a prefix
func (cckv *CancelConsulKV) Keys(
	ctx context.Context,
	prefix string,
	separator string,
	q *consul.QueryOptions,
) ([]string, *consul.QueryMeta, error) {
	keysCh := make(chan []string, 1)
	metaCh := make(chan *consul.QueryMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		cckv.Logger.WithField("prefix", prefix).Debug(" => Keys")
		keys, meta, err := cckv.Client.KV().Keys(prefix, separator, q)
		cckv.Logger.WithFields(logrus.Fields{
			"prefix":        prefix,
			"options":       q,
			"keys":          keys,
			"meta":          meta,
			logrus.ErrorKey: err,
		}).Debug(" <= Keys")
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
		return nil, nil, ctx.Err()
	}
}

// Put writes a key-value pair to the store
func (cckv *CancelConsulKV) Put(
	ctx context.Context,
	p *consul.KVPair,
	q *consul.WriteOptions,
) (*consul.WriteMeta, error) {
	metaCh := make(chan *consul.WriteMeta, 1)
	errCh := make(chan error, 1)
	go func() {
		cckv.Logger.WithField("key", p.Key).Debug(" => Put")
		meta, err := cckv.Client.KV().Put(p, q)
		cckv.Logger.WithFields(logrus.Fields{
			"key":           p.Key,
			"kv":            p,
			"options":       q,
			"meta":          meta,
			logrus.ErrorKey: err,
		}).Debug(" <= Put")
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
		return nil, ctx.Err()
	}
}
