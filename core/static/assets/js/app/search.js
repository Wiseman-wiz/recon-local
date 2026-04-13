var search_input = document.querySelector("#search-filter");

var filter_reports = function(e){
    var report_list = document.querySelector("#rendered-list");
    const term = e.target.value.toLowerCase();
    const report_cards = report_list.getElementsByTagName("section")
    if (term.length > 0) {
        document.querySelector(".btn-show-less").classList.add("d-none")
        document.querySelector(".btn-show-more").classList.add("d-none")
    } else {
        document.querySelector(".btn-show-less").classList.remove("d-none")
        document.querySelector(".btn-show-more").classList.remove("d-none")
    }
    Array.from(report_cards).forEach(function(report){
        if (report.id.toLowerCase().indexOf(term) != -1) {
            report.classList.add("d-block")
            report.classList.remove("d-none")
        } else {
            report.classList.add("d-none")
            report.classList.remove("d-block")
        }
    })
}


search_input.addEventListener('keyup', filter_reports);
