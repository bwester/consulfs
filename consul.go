package consulfs

import (
	consul "github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// ConsulCanceler defines an API for accessing a Consul Key-Value store. It's mostly a
// clone of `github.com/hashicorp/consul/api.KV`, but it adds a parameter for a context
// to method signatures.
//
// Using this interface is deprecated. As of v0.9.0, the official Consul API package
// allows API calls be canceled using a Context, making this interface unnecessary.
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
// `Client` object and performs all operations using that client.
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
	cckv.Logger.WithField("key", p.Key).Debug(" => CAS")
	success, meta, err := cckv.Client.KV().CAS(p, q.WithContext(ctx))
	cckv.Logger.WithFields(logrus.Fields{
		"key":           p.Key,
		"kv":            p,
		"success":       success,
		"meta":          meta,
		logrus.ErrorKey: err,
	}).Debug(" <= CAS")
	return success, meta, err
}

// Delete removes a key and its data.
func (cckv *CancelConsulKV) Delete(
	ctx context.Context,
	key string,
	w *consul.WriteOptions,
) (*consul.WriteMeta, error) {
	cckv.Logger.WithField("key", key).Debug(" => Delete")
	meta, err := cckv.Client.KV().Delete(key, w.WithContext(ctx))
	cckv.Logger.WithFields(logrus.Fields{
		"key":           key,
		"options":       w,
		"meta":          meta,
		logrus.ErrorKey: err,
	}).Debug(" <= Delete")
	return meta, err
}

// Get returns the current value of a key.
func (cckv *CancelConsulKV) Get(
	ctx context.Context,
	key string,
	q *consul.QueryOptions,
) (*consul.KVPair, *consul.QueryMeta, error) {
	cckv.Logger.WithField("key", key).Debug(" => Get")
	pair, meta, err := cckv.Client.KV().Get(key, q.WithContext(ctx))
	cckv.Logger.WithFields(logrus.Fields{
		"key":           key,
		"options":       q,
		"kv":            pair,
		"meta":          meta,
		logrus.ErrorKey: err,
	}).Debug(" <= Get")
	return pair, meta, err
}

// Keys lists all keys under a prefix
func (cckv *CancelConsulKV) Keys(
	ctx context.Context,
	prefix string,
	separator string,
	q *consul.QueryOptions,
) ([]string, *consul.QueryMeta, error) {
	cckv.Logger.WithField("prefix", prefix).Debug(" => Keys")
	keys, meta, err := cckv.Client.KV().Keys(prefix, separator, q.WithContext(ctx))
	cckv.Logger.WithFields(logrus.Fields{
		"prefix":        prefix,
		"options":       q,
		"keys":          keys,
		"meta":          meta,
		logrus.ErrorKey: err,
	}).Debug(" <= Keys")
	return keys, meta, err
}

// Put writes a key-value pair to the store
func (cckv *CancelConsulKV) Put(
	ctx context.Context,
	p *consul.KVPair,
	q *consul.WriteOptions,
) (*consul.WriteMeta, error) {
	cckv.Logger.WithField("key", p.Key).Debug(" => Put")
	meta, err := cckv.Client.KV().Put(p, q.WithContext(ctx))
	cckv.Logger.WithFields(logrus.Fields{
		"key":           p.Key,
		"kv":            p,
		"options":       q,
		"meta":          meta,
		logrus.ErrorKey: err,
	}).Debug(" <= Put")
	return meta, err
}
