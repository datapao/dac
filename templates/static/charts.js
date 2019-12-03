function chart(chart_id, title, data_label, type, fill, labels, data) {
    var ctx = document.getElementById(chart_id).getContext('2d');
    var chart_setup = {
        type: type,
        data: {
            labels: labels,
            datasets: [{
                label: data_label,
                data: data,
                fill: fill,
                backgroundColor: '#7B8CDE',
                borderColor: '#7B8CDE',
                borderWidth: 1
            }]
        },
        options: {
            scales: {
                yAxes: [{
                    ticks: {
                        beginAtZero: true
                    }
                }]
            }
        }
    };
    if (title != false) {
        chart_setup['options']['title'] = {
            display: true,
            text: title
        }
    };

    var myChart = new Chart(ctx, chart_setup);
  }