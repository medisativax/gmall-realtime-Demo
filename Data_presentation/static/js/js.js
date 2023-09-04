$(window).load(function () {
  $(".loading").fadeOut();
});
$(function () {
  echarts_1();
  echarts_3();
  echarts_21();
  echarts_6();
  echarts_7();

  function echarts_1() {
    // 基于准备好的dom，初始化echarts实例
    var myChart = echarts.init(document.getElementById("echart1"));

    var option = {
      tooltip: {
        trigger: "item",
      },
      series: [
        {
          name: "各省份购买人数",
          type: "pie",
          radius: "80%",
          data: [
            { value: 0, name: "c.c" },
            { value: 0, name: "c.c" },
          ],
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
              shadowColor: "rgba(0, 0, 0, 0.5)",
            },
          },
        },
      ],
    };
    myChart.setOption(option);
    setInterval(makeAjaxRequest1, 2000);

    function makeAjaxRequest1() {
      $.ajax({
        url: "http://192.168.0.199:8070/api/pyecharts/provincecount",
        type: "GET",
        dataType: "JSON",
        success: function (res) {
          option.series[0].data = res.data;
          // 使用刚指定的配置项和数据显示图表。
          myChart.setOption(option);
        },
      });
    }

    // 使用刚指定的配置项和数据显示图表。
    window.addEventListener("resize", function () {
      myChart.resize();
    });
  }

  function echarts_21() {
    // 基于准备好的dom，初始化echarts实例
    var myChart = echarts.init(document.getElementById("echart21"));
    var option = {
      grid: {
        top: "10",
        left: "0",
        right: "5%",
        bottom: "0",
        containLabel: true,
      },
      yAxis: [
        {
          type: "category",
          data: ["textone", "textwo", "texthree", "textfour"],
          inverse: true,
          axisTick: { show: false },
          axisLabel: {
            textStyle: {
              color: "rgba(255,255,255,.3)",
            },
          },
          axisLine: {
            show: false,
          },
        },
      ],
      xAxis: [
        {
          type: "value",
          axisLabel: {
            show: false,
          },
          axisLine: {
            show: false,
          },
          splitLine: {
            show: false,
          },
        },
      ],
      series: [
        {
          type: "bar",
          barWidth: 10,
          data: [0, 0, 0, 0],
          label: {
            normal: {
              show: true,
              position: "insideBottomRight",
              formatter: "{c}",
              distance: 0,
              offset: [10, -15],
              color: "#fff",
              fontSize: 12,
              padding: [2, 5, 2, 5],
              backgroundColor: {
                image:
                  "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAIwAAAA+CAYAAAD5wvNAAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA0xpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuNi1jMDY3IDc5LjE1Nzc0NywgMjAxNS8wMy8zMC0yMzo0MDo0MiAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0UmVmPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VSZWYjIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6RjQ3NTQ1RkVGOUM1MTFFOEJCQTdENzhFNjM5MzM3NkYiIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6RjQ3NTQ1RkRGOUM1MTFFOEJCQTdENzhFNjM5MzM3NkYiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENDIDIwMTUgKE1hY2ludG9zaCkiPiA8eG1wTU06RGVyaXZlZEZyb20gc3RSZWY6aW5zdGFuY2VJRD0iYWRvYmU6ZG9jaWQ6cGhvdG9zaG9wOmE5M2UxZjIxLTQyMmYtMTE3Yy05MTVlLWVhNzA0NDUwYzIzOSIgc3RSZWY6ZG9jdW1lbnRJRD0iYWRvYmU6ZG9jaWQ6cGhvdG9zaG9wOmE5M2UxZjIxLTQyMmYtMTE3Yy05MTVlLWVhNzA0NDUwYzIzOSIvPiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/PvulhDwAAAQ3SURBVHja7J1JaBRBGIX/iZEQN1CjwVxUcL+4hUnUU8SDHjQiiqJR0EOCYMAtIG4HRQXBBUQkXgQxxoOieJZ4UzO4oAe3CKJoQI2IGhUTZHz/dI12mul090xcJvU+ePSkpqsTXr1Ud81Ud8UW1SZFYuJgtsn0z+Iqd+2TFPHWGY1tFVSJ96ZgOx5lo7AdDBX9OmbMczx3mas8076BZQUhfoe3ns+xve/7/X6JcpyIZRK17u+2+I7tF5S/w+vneP0YuoXX16G33mMk5kkkCiV7RkLroNXQbE/Tk39HkdEIaDK00JTr//kd6Dx0FnqfzcELsqgzBjoCvYCOQuUMS14QM22lbfYSOgGV/cnAaG/UAD2FtkKD2QZ5yyBoE/QkfkMaoIF9HZiJerqDDkND6He/YYhp0wRCM62vAlMN3YZm0t9+ywyoNX5TluYamFroEjSMnlrR21xEaGqzDcxGqBEaQC+tQdu6MX4r1faRArPEXEUTOzkRb01lIFRgpkJN7Fms72maEJqpQYHR4VUzR0LEZKA5nug55PYGZjM0nV4Rg2Zhi19g9FO/vfSIeNiDXqYsU2B28lREfE5Nu7yBKYE20Bviw3r0MiXuwNRAxfSF+FBsMtIjMIT0xpp0YEZDs+gHCWC2XvxqYOYL57OQYDQjVRqYCnpBQlKugZlCH0hIJmtgJtAHEpKJGphS+kBCUqqBGUofSEiGFtADEgUGhjAwhIEhDAzJQ7o0MJ/pAwnJZw3MG/pAQvJBA/OMPpCQtGlgHtMHEpInGphW+kBCckcDo08mStILEoBmpCV90XuXfpAA7ibi0p7+HOYc/SAB6KPOxB2Yb/SE+KDZOOsOTAd0hr4QH87gdNThDoxyAOqkN8RDp8mGeAPTDu2jP8TDfr3YzRQY5Th0jx4Rg2bhmLvAG5huaCX0iV5Zj34pvQq9S3dvgVHaoLXQD3pmLdr2NYmK1DOZJSgwylWonr5ZSz3CcjXTG71NoDoF1bGnsa5nqUtUptpeogZGOQ0t5zWNNcPnFYk5qTaXbAOjXBFnUQOOnvov96EKhOVy0I5h5/TqhXClOI+u4lcI/Qdty91QPDFXHoapEGUSeBd0UJyFKnRs/oV+5y1foZPQJATlANQVtmI2dw28Fmf5m3Fmq4s2cT7N/096gS1ts7HiLH/zKupBclmRrcP0NCq9ob9KnGfN6NOjnSX8nCcwDmRb/VW6zQWss4SfyCNxZlXqRLmcJ/wX9tEfqX/IBaN84RC0I8dj6Opm22xKo803sulziZtyqK91t9tmms2B0XO6Ppv4WhZ1W0zdJANjFzo6qJZod07o/Odlpq4wMHYOMRdLuBv6dB9d1vejrWYxMA46olhktrnsw8BYhPYeC3x6j84IvRADYxEPMlyfZHOdw8BYhHsElB5JtdAWh0JakBH9jGW4OJ9UN9GO3/wUYAAaXtVsjsG1HQAAAABJRU5ErkJggg==",
              },
            },
          },
          itemStyle: {
            normal: {
              color: new echarts.graphic.LinearGradient(
                1,
                0,
                0,
                0,
                [
                  {
                    offset: 0,
                    color: "#57eabf", // 0% 处的颜色
                  },
                  {
                    offset: 1,
                    color: "#2563f9", // 100% 处的颜色
                  },
                ],
                false
              ),
              barBorderRadius: 14,
            },
          },
        },
        {
          type: "bar",
          barWidth: 10,
          xAxisIndex: 0,
          barGap: "-100%",
          data: [100, 100, 100, 100],
          itemStyle: {
            normal: {
              color: "#444a58",
              barBorderRadius: 14,
            },
          },
          zlevel: -1,
        },
      ],
    };
    myChart.setOption(option);

    setInterval(makeAjaxRequest2, 2000);

    function makeAjaxRequest2() {
      $.ajax({
        url: "http://192.168.0.199:8070/api/pyecharts/channels",
        type: "GET",
        dataType: "JSON",
        success: function (res) {
          option.yAxis[0].data = [
            res.data[0].type,
            res.data[1].type,
            res.data[2].type,
            res.data[3].type,
            res.data[4].type,
          ];
          option.series[0].data = [
            res.data[0].value,
            res.data[1].value,
            res.data[2].value,
            res.data[3].value,
            res.data[4].value,
          ];
          option.series[1].data = [
            res.data[0].value + 50,
            res.data[1].value + 50,
            res.data[2].value + 50,
            res.data[3].value + 50,
            res.data[4].value + 50,
          ];
          // 使用刚指定的配置项和数据显示图表。
          myChart.setOption(option);
        },
      });
    }

    window.addEventListener("resize", function () {
      myChart.resize();
    });
  }

  function echarts_3() {
    // 基于准备好的dom，初始化echarts实例
    var myChart = echarts.init(document.getElementById("echart3"));
    var keywords = [
      { name: "C.C.", value: 2.64 },
      { name: "C.C.", value: 4.03 },
      { name: "C.C.", value: 24.95 },
      { name: "C.C.", value: 4.04 },
      { name: "C.C.", value: 5.27 },
      { name: "C.C.", value: 5.8 },
      { name: "C.C.", value: 3.09 },
    ];

    var option = {
      series: [
        {
          type: "wordCloud",
          //maskImage: maskImage,
          sizeRange: [15, 80],
          rotationRange: [0, 0],
          rotationStep: 45,
          gridSize: 15,
          shape: "pentagon",
          width: "100%",
          height: "100%",
          textStyle: {
            normal: {
              color: function () {
                return (
                  "rgb(" +
                  [
                    Math.round(Math.random() * 200 + 55),
                    Math.round(Math.random() * 200 + 55),
                    Math.round(Math.random() * 200 + 55),
                  ].join(",") +
                  ")"
                );
              },
              fontFamily: "sans-serif",
              fontWeight: "normal",
            },
            emphasis: {
              shadowBlur: 10,
              shadowColor: "#333",
            },
          },
          data: keywords,
        },
      ],
    };
    setInterval(makeAjaxRequest3, 2000);

    function makeAjaxRequest3() {
      $.ajax({
        url: "http://localhost:8070/api/pyecharts/wordCloud",
        type: "GET",
        dataType: "JSON",
        success: function (res) {
          option.series[0].data = res.data;
          // 使用刚指定的配置项和数据显示图表。
          myChart.setOption(option);
        },
      });
    }

    // 使用刚指定的配置项和数据显示图表。
    myChart.setOption(option);
    window.addEventListener("resize", function () {
      myChart.resize();
    });
  }

  function echarts_6() {
    var myChart = echarts.init(document.getElementById("echart6"));

    var option = {
      color: [
        "#67F9D8",
        "#FFE434",
        "#56A3F1",
        "#FF917C",
        "ef233c",
        "#a2d2ff",
        "#bde0fe",
        "#ffafcc",
        "#ffc8dd",
        "#cdb4db",
        "#4cc9f0",
      ],
      legend: {
        show: true,
        type: "plain",
        icon: "rect",
        bottom: 0,
        itemGap: 15,
        textStyle: {
          color: "red",
          fontWeight: "bold",
        },
      },
      radar: {
        //   shape: 'circle',
        indicator: [
          { name: "销售量", max: 400 },
          { name: "用户量", max: 200 },
          { name: "总收益", max: 4000000 },
          { name: "退单量", max: 15 },
          { name: "退单用户量", max: 15 },
        ],
        center: ["50%", "50%"],
        radius: 180,
        splitNumber: 5,
        name: {
          // (圆外的标签)雷达图每个指示器名称的配置项。
          formatter: "{value}",
          textStyle: {
            fontSize: 15,
          },
        },
      },
      series: [
        {
          name: "Budget vs spending",
          type: "radar",
          itemStyle: {
            //设置折线拐点标志的样式
            normal: { lineStyle: { width: 3 }, opacity: 0.2 }, //设置普通状态时的样式
            emphasis: { lineStyle: { width: 6 }, opacity: 1 }, //设置高亮时的样式
          },
          data: [
            {
              value: [72, 57, 16409, 1, 1],
              name: "索芙特",
              label: {
                normal: {
                  show: true,
                  formatter: function (params) {
                    return params.value;
                  },
                  textStyle: {
                    fontSize:15
                  }
                },
              }
            },
            {
              value: [77, 65, 1296423, 1, 1],
              name: "苹果",
              label: {
                normal: {
                  show: true,
                  formatter: function (params) {
                    return params.value;
                  },
                  textStyle: {
                    fontSize:15
                  }
                },
              }
            },
          ],
        },
      ],
    };

    setInterval(makeAjaxRequest3, 2000);

    function makeAjaxRequest3() {
      $.ajax({
        url: "http://localhost:8070/api/pyecharts/trademark?date=20230624",
        type: "GET",
        dataType: "JSON",
        success: function (res) {
          option.series[0].data = res.data;
          option.series[0].label = {
            normal: {
              show: true,
              formatter: function (params) {
                return params.value;
              },
              textStyle: {
                fontSize: 15
              }
            }
          };
          // 使用刚指定的配置项和数据显示图表。
          myChart.setOption(option);
        },
      });
    }
    myChart.setOption(option);
    window.addEventListener("resize", function () {
      myChart.resize();
    });
  }

  function echarts_7() {
    var myChart = echarts.init(document.getElementById("echart7"));

    // myChart.setOption(option);
    window.addEventListener("resize", function () {
      myChart.resize();
    });
  }
});
