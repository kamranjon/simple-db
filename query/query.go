package query

type Query struct {
	SelectColumns []string
	OrderColumns []string
	FilterClause map[string]interface{}
}

func NewQuery() Query{
	return Query{}
}

func (query Query) Select(columns ...string) Query{
	query.SelectColumns = columns
	return query
}

func (query Query) Order(columns ...string) Query{
	query.OrderColumns = columns
	return query
}

func (query Query) Filter(clause map[string]interface{}) Query{
	query.FilterClause = clause
	return query
}

