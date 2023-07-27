package sql

import (
	"fmt"
	"strings"

	"github.com/alibaba/ilogtail/pkg/models"
	"github.com/alibaba/ilogtail/pkg/pipeline"
)

type ProcessorSQL struct {
	SourceKey       string
	KeepSource      bool
	ExpandConnector string
	ExpandDepth     int
	Script          string

	context pipeline.Context
}

const pluginName = "processor_sql"

func (p *ProcessorSQL) Init(context pipeline.Context) error {
	if p.SourceKey == "" {
		return fmt.Errorf("must specify SourceKey for plugin %v", pluginName)
	}
	p.context = context
	return nil
}

func (*ProcessorSQL) Description() string {
	return "json processor for logtail"
}

func (p *ProcessorSQL) Process(in *models.PipelineGroupEvents, context pipeline.PipelineContext) {
	for _, event := range in.Events {
		p.processEvent(event)
	}
	context.Collector().Collect(in.Group, in.Events...)
}
func init() {
	pipeline.Processors[pluginName] = func() pipeline.Processor {
		return &ProcessorSQL{
			SourceKey:       "",
			ExpandDepth:     0,
			ExpandConnector: "_",
			Script:          "",
			KeepSource:      true,
		}
	}
}

// select name,age from log
//
//	{
//		"name":"a",
//		"age":1,
//		"sex":0
//	}
//
//	{
//		"name":"b",
//		"age":2,
//		"sex":1
//	}
func (p *ProcessorSQL) processEvent(event models.PipelineEvent) {
	script := p.Script
	s := strings.Split(script, "from")
	se := s[0]
	s2 := strings.Split(se, "select")
	s3 := s2[1]
	filed := strings.Split(s3, ",")
	contents := event.(*models.Log).GetIndices()
	val := contents.Get(p.SourceKey)
	fmt.Println("file:%v,val:%v", filed, val)
}
