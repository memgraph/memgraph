(() => {
  'use strict';

  class QpsGauge {
    constructor(canvas) {
      this.gauge = new Gauge(canvas).setOptions({
        lines: 12, // The number of lines to draw
        angle: 0.07, // The length of each line
        lineWidth: 0.27, // The line thickness
        pointer: {
          length: 0.82, // The radius of the inner circle
          strokeWidth: 0.073, // The rotation offset
          color: '#000' // Fill color
        },
        // If true, the pointer will not go past the end of the gauge
        limitMax: 'true',   
        strokeColor: '#e0e0e0', // to see which ones work best for you
        generateGradient: true,
        percentColors: [[0.0, '#d32f2f'], [0.5, '#ffee58'], [1.0, '#388e3c']],
      });

      this.gauge.animationSpeed = 1;
    }

    set(value) {
      this.gauge.set(value);
    }

    setMax(value) {
      this.gauge.maxValue = this.maxValue = value;
    }

    getMax() {
      return this.maxValue;
    }
  }

  class CypherQuery {
    constructor(textarea) {
      this.textarea = textarea;
      this.editor = CodeMirror.fromTextArea(this.textarea, {
        height: 0,
        lineNumbers: true,
        mode: 'application/x-cypher-query',
        indentWithTabs: false,
        smartIndent: true,
        matchBrackets: true,
        theme: 'neo',
        viewportMargin: Infinity
      });
    }

    set(value) {
      this.editor.getDoc().setValue(value);
    }

    get(separator) {
      separator = separator ? separator : ' ';
      this.editor.getDoc().getValue(separator);
    }
  }

  class QpsText {
    constructor(element) {
      this.element = element;
    }

    get() {
      $(this.element).text();
    }

    set(text) {
      $(this.element).text(text);
    }
  }

  class QueryCard {
    constructor(card, maxQps, value) {
      this.card = card;
      this.maxQps = maxQps;

      value = value ? value : 1;
      
      this.text = new QpsText($(card).find('#text')[0]);
      this.gauge = new QpsGauge($(card).find('#gauge')[0]);
      this.query = new CypherQuery($(card).find('#query')[0]);

      this.gauge.setMax(maxQps + 1);
      this.gauge.animationSpeed = 1;
      this.gauge.set(value);
    }

    set_value(value) {
      this.text.set(value);
      this.gauge.set(value);
    }

    set_query(query) {
      this.query.set(query);
    }
  }

  // put \n on module 2 space positions
  var put_new_line_mod_2 = function(array) {
    let join_array = array.map(function(o, i) {
      if (i % 2 === 0)
        return ' ';
      else
        return '\n';
    });
    join_array.pop();
    return array.map(function(v,i) {
      return [v, join_array[i]]; 
    }).reduce(function(a,b) {
      return a.concat(b); 
    });
  };

  // --  variable part  ----
  let running = false;
  let value = 0;
  let maxQps = 10000;
  let memgraphCards = [];
  let neo4jCards = [];
  let memgraphSec = 0;
  let neo4jSec = 0;
  let memgraphLine = [];
  let neo4jLine = [];
  var queries = [
    "CREATE (n:Item{id:@}) RETURN n",
    "MATCH (n:Item{id:#}),(m:Item{id:#}) CREATE (n)-[r:test]->(m) RETURN r",
    "MATCH (n:Item{id:#}) SET n.prop=# RETURN n",
    "MATCH (n:Item{id:#}) RETURN n",
    "MATCH (n:Item{id:#})-[r]->(m) RETURN count(r)"
  ];
  var params = {
    host: "localhost",
    port: "7474",
    connections: 2,
    duration: 1,
    queries: queries
  };
  // -----------------------

  // server control functions
  var start = function() {
    $.ajax({url:'/start', type:"POST", success: function(data){}});
  };
  var stop = function() {
    $.ajax({url:'/stop', type:"POST", success: function(data){}});
  };
  var registerParams = function(f) {
    $.ajax({
      url:'/params', type:"POST", data:JSON.stringify(params),
      contentType:"application/json; charset=utf-8",
      success: function(data){
        f();
      }
    });
  };
  registerParams();

  // setup cards
  queries.forEach(function(query, i) {
    query = put_new_line_mod_2(query.split(" ")).join('');
    neo4jCards.push(new QueryCard($('#q-0-' + i.toString())[0], maxQps));
    memgraphCards.push(new QueryCard($('#q-1-' + i.toString())[0], maxQps));
    neo4jCards[i].set_query(query);
    memgraphCards[i].set_query(query);
  });

  // start stop button
  $("#run-neo4j").click(function() {
    running = !running;
    if (running) {
      $(this).text('Stop Neo4j');
      let hostname_port = $("#neo4j_url").val().split(":");
      params.host = hostname_port[0];
      params.port = hostname_port[1];
      params.connections = 2;
      registerParams(function() {
        start();
        updateNeo4j();
      });
    }
    if (!running) {
      $(this).text('Start Neo4j');
      stop();
    }
  });

   // start stop button
  $("#run-memgraph").click(function() {
    running = !running;
    if (running) {
      $(this).text('Stop Memgraph');
      let hostname_port = $("#memgraph_url").val().split(":");
      params.host = hostname_port[0];
      params.port = hostname_port[1];
      params.connections = 4;
      registerParams(function() {
        start();
        updateMemgraph();
      });
    }
    if (!running) {
      $(this).text('Start Memgraph');
      stop();
    }
  });

  // update only line on the graph 
  var updateGraph = function() {
      let newData = [{
        values: memgraphLine,
        key: 'Memgraph',
        color: '#ff0000',
        strokeWidth: 3
      }, {
        values: neo4jLine,
        key: 'Neo4j',
        color: '#0000ff',
        strokeWidth: 3
      }];
      chartData.datum(newData).transition().duration(500).call(chart);
  };

  // update
  function updateNeo4j() {
    if (!running) {
      stop();
      return;
    }
    setTimeout(() => {
      $.ajax({
        url:'/stats', type:"GET",
        success: function(data){
          if (!data || !data.total || !data.per_query)
            return;

          neo4jSec = neo4jSec + 1;
          neo4jLine.push({x: neo4jSec, y: data.total});
          data.per_query.forEach(function(speed, i) {
            neo4jCards[i].set_value(Math.round(speed));
          });
          updateGraph();
        }
      });
      updateNeo4j();
    }, 1000);
  }

  // update
  function updateMemgraph() {
    if (!running) {
      stop();
      return;
    }
    setTimeout(() => {
      $.ajax({
        url:'/stats', type:"GET",
        success: function(data){
          if (!data || !data.total || !data.per_query)
            return;

          memgraphSec = memgraphSec + 1;
          memgraphLine.push({x: memgraphSec, y: data.total});
          data.per_query.forEach(function(speed, i) {
            memgraphCards[i].set_value(Math.round(speed));
          });
          updateGraph();
        }
      });
      updateMemgraph();
    }, 1000);
  }

  // graph init
  var chart;
  var chartData;
  nv.addGraph(function() {
    chart = nv.models.lineChart()
         .interpolate('basis')
         .useInteractiveGuideline(true)
         .showLegend(true)
         .showYAxis(true)
         .showXAxis(true);

    chart.xAxis
         .axisLabel('Time (s)')
         .tickFormat(d3.format(',r'));

    chart.yAxis
         .axisLabel('QPS')
         .tickFormat(d3.format('f'));

    chart.forceY([0, 50000]);

    chartData = d3.select('#chart svg')
      .datum([]);
    chartData.call(chart);

    chart.update();
    nv.utils.windowResize(function() { chart.update(); });
    return chart;
  });

})();
