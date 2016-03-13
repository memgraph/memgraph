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

  let value = 0;
  let maxQps = 5000;

  let card1 = new QueryCard($('#q1')[0], maxQps);
  let card2 = new QueryCard($('#q2')[0], maxQps);
  let card3 = new QueryCard($('#q3')[0], maxQps);
  let card4 = new QueryCard($('#q4')[0], maxQps);
  let card5 = new QueryCard($('#q5')[0], maxQps);
  let card6 = new QueryCard($('#q6')[0], maxQps);
  let card7 = new QueryCard($('#q7')[0], maxQps);
  let card8 = new QueryCard($('#q8')[0], maxQps);

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


  nv.addGraph(function() {
    var chart = nv.models.lineChart()
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

    var myData = sinAndCos();

    d3.select('#chart svg')
      .datum(myData)
      .call(chart);

    chart.update();
    nv.utils.windowResize(function() { chart.update(); });
    return chart;
  });

  function sinAndCos() {
    var sin = [],sin2 = [],
        cos = [];

    for (var i = 0; i < 100; i++) {
      sin.push({x: i, y: Math.sin(i/10)});
      sin2.push({x: i, y: Math.sin(i/10) *0.25 + 0.5});
      cos.push({x: i, y: 0.5 * Math.cos(i/10)});
    }

    return [{
        values: sin,
        key: 'Sine Wave',
        color: '#ff7f0e'
      }, {
        values: cos,
        key: 'Cosine Wave',
        color: '#2ca02c'
      }, {
        values: sin2,
        key: 'Another sine wave',
        color: '#7777ff',
        area: true
      }];
  }

})();
