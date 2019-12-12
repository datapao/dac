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


function stacked_chart(chart_id, title, type, fill, labels, interactive_label, interactive_data, job_label, job_data) {
    var ctx = document.getElementById(chart_id).getContext('2d');
    var chart_setup = {
        type: type,
        data: {
            labels: labels,
            datasets: [
            {
                label: interactive_label,
                data: interactive_data,
                fill: fill,
                backgroundColor: '#7B8CDE',
                borderColor: '#7B8CDE',
                borderWidth: 2
            },
            {
                label: job_label,
                data: job_data,
                fill: fill,
                backgroundColor: '#eda151',
                borderColor: '#eda151',
                borderWidth: 2
            }
        ]
        },
        options: {
            scales: {
                yAxes: [{
                    ticks: {
                        beginAtZero: true
                    },
                    stacked: true
                }],
                xAxes: [{ stacked: true }]
            }
        }
    };
    if (title != false) {
        chart_setup['options']['title'] = {
            display: true,
            text: title
        }
    };

    // remove huge dots
    if (fill) chart_setup['options']['elements'] = { point: { radius: 1 } };

    var myChart = new Chart(ctx, chart_setup);
}