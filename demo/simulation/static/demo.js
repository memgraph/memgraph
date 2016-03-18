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

    set(value) {
      this.text.set(value);
      this.gauge.set(value);
    }
  }

  // counters init
  let value = 0;
  let maxQps = 15000;

  let card1 = new QueryCard($('#q1')[0], maxQps);
  let card2 = new QueryCard($('#q2')[0], maxQps);
  let card3 = new QueryCard($('#q3')[0], maxQps);
  let card4 = new QueryCard($('#q4')[0], maxQps);
  let card5 = new QueryCard($('#q5')[0], maxQps);
  let card6 = new QueryCard($('#q6')[0], maxQps);
  let card7 = new QueryCard($('#q7')[0], maxQps);
  let card8 = new QueryCard($('#q8')[0], maxQps);

  // counters update
  function run() {
    setTimeout(() => {
      value += 10;

      if(value >= maxQps)
        value = 0;
        
      card1.set(value);
      card2.set(value);
      card3.set(value);
      card4.set(value);
      card5.set(value);
      card6.set(value);
      card7.set(value);
      card8.set(value);

      run();
    }, 20);
  }
  run();

  // graph init
  var data = [];
  var chart;
  var chartData;
  nv.addGraph(function() {
    chart = nv.models.lineChart()
         .useInteractiveGuideline(true)
         .showLegend(true)
         .showYAxis(true)
         .showXAxis(true);

    chart.xAxis
         .axisLabel('Time (s)')
         .tickFormat(d3.format(',r'));

    chart.yAxis
         .axisLabel('QPS')
         .tickFormat(d3.format(',r'));

    chartData = d3.select('#chart svg')
      .datum(data);
    chartData.call(chart);

    chart.update();
    nv.utils.windowResize(function() { chart.update(); });
    return chart;
  });

  // graph update
  let x = 0;
  function updateGraph() {
    setTimeout(() => {
      x += 1;
      if (x > 100)
        x = 0
      var memgraphLine = [];
      var neo4jLine = [];
      for (var i = 0; i < x; i++) {
        memgraphLine.push({x: i, y: 100 * Math.random() + 1000});
        neo4jLine.push({x: i, y: 100 * Math.random() + 50});
      }
      var newData = [{
        values: memgraphLine,
        key: 'Memgraph',
        color: '#ff0000'
      }, {
        values: neo4jLine,
        key: 'Neo4j',
        color: '#0000ff'
      }];
      chartData.datum(newData).transition().duration(500).call(chart);
      updateGraph();
    }, 1000);
  }
  updateGraph();

})();
