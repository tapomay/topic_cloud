function cloudify(wrdWeightMap) {

  var d3 = require("d3");
  var cloud = require(".");

  //var fill = d3.scale.category20();

  var layout = cloud();
  layout.size([500, 500])
  // var wrdList = ["Hello", "world", "normally", "you", "want", "more", "words", "than", "this"];
  // var wrdList = arr;
  
  // function wrdSort(d) {
  //     return {text: d, size: 10 + Math.random() * 90};
  // }
  // wrdList.map(wrdSort);

  // var words = [];
  // console.log("R2D2: " + wrdWeightMap)

  // for (var wrdStr in wrdWeightMap) {
  //   console.log("K:" + wrdStr);
  //   console.log("V: " + wrdWeightMap[wrdStr]);
  //   var wrdObj = {text: wrdStr, size: 10 + wrdWeightMap[wrdStr] * 90};
  //   words.push(wrdObj);
  // }

  //TEMP...
// var getR = function() {return 10 + Math.random() * 90};
// var getS = function() {
//   var obj = {"amazing":getR(),"brilliant":getR(),"extravagant":getR()};
//   var ret = JSON.stringify(obj);
//   return obj;
// }
  // wrdWeightMap = getS();
  //...TEMP
  var words = Object.keys(wrdWeightMap).map((wrdStr) => {return {text: wrdStr, size: 10 + wrdWeightMap[wrdStr] * 90}});
  console.log("CLOUD: " + JSON.stringify(Object.keys(wrdWeightMap)));
  console.log("CLOUD: " + JSON.stringify(wrdWeightMap['amazing']));
  
  layout.words(words);
  layout.padding(5);
  layout.rotate(function() { return ~~(Math.random() * 2) * 90; });
  layout.font("Impact");
  layout.fontSize(function(d) { return d.size; });
  layout.on("end", draw);

  layout.start();

  function draw(words) {
    d3.select("svg").remove();
    d3.select("#server-cloud").append("svg")
        .attr("width", layout.size()[0])
        .attr("height", layout.size()[1])
      .append("g")
        .attr("transform", "translate(" + layout.size()[0] / 2 + "," + layout.size()[1] / 2 + ")")
      .selectAll("text")
        .data(words)
      .enter().append("text")
        .style("font-size", function(d) { return d.size + "px"; })
        .style("font-family", "Impact")
  //      .style("fill", function(d, i) { return fill(i); })
        .attr("text-anchor", "middle")
        .attr("transform", function(d) {
          return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
        })
        .text(function(d) { return d.text; });
  }
};

module.exports.cloudify = cloudify;