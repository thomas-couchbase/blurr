package databases

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

type RestClient struct {
	client  *http.Client
	baseURI string
}

func (c RestClient) Do(q string) error {
	data := bytes.NewReader([]byte(q))
	req, err := http.NewRequest("POST", c.baseURI, data)
	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	resp, err = c.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)

	return err
}

type Tuq struct {
	client *RestClient
}

const MaxIdleConnsPerHost = 1000

func (cb *Tuq) Init(config Config) {
	baseURI := fmt.Sprintf("%squery", config.Addresses[0])
	t := &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
	cb.client = &RestClient{&http.Client{Transport: t}, baseURI}
}

func (t *Tuq) Shutdown() {}

func (t *Tuq) Create(key string, value map[string]interface{}) error {
	return nil
}

func (t *Tuq) Read(key string) error {
	return nil
}

func (t *Tuq) Update(key string, value map[string]interface{}) error {
	return nil
}

func (t *Tuq) Delete(key string) error {
	return nil
}

func (t *Tuq) Query(key string, args []interface{}) error {
	view := args[0].(string)
	arg := args[1].(string)

	var q string
	if view == "id_by_city" {
		q = fmt.Sprintf("select category from default where city.f.f = \"%s\"", arg)
	}
	return t.client.Do(q)
}
