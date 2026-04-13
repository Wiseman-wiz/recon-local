var dTblSettings = {
  dom: "Blfrtip",
  stateSave: true,
  lengthMenu: [
    [10, 25, 50, 100, -1],
    ["10 rows", "25 rows", "50 rows", "100 rows", "Show all"],
  ],
  lengthChange: false,
  responsive: true,
  language: {
    buttons: { colvisRestore: "RESET COLUMN" },
  },
  buttons: [
    {
      extend: "excel",
      filename: function () {
        return document.querySelector("#report-name").textContent;
      },
      exportOptions: {
        columns: function (idx, data, node) {
          if ($(node).hasClass("noVis")) {
            return false;
          }
          tableID = $(node).closest("table").attr("id");
          return tableID === undefined
            ? false
            : $(tableID).DataTable().column(idx).visible();
        },
      },
    },
    {
      extend: "csv",
      filename: function () {
        return document.querySelector("#report-name").textContent;
      },
      exportOptions: {
        columns: function (idx, data, node) {
          if ($(node).hasClass("noVis")) {
            return false;
          }
          tableID = $(node).closest("table").attr("id");
          return tableID === undefined
            ? false
            : $(tableID).DataTable().column(idx).visible();
        },
      },
    },
    {
      extend: "colvis",
      postfixButtons: ["colvisRestore"],
    },
    {
      extend: "searchBuilder",
      columns: true,
      layout: {
        top1: "searchBuilder",
      },
    },
    "pageLength",
  ],
  columnDefs: [
    {
      orderable: false,
      targets: [0, 1],
    },
  ],
};

function initializeDataTable(selector, settings) {
  dTbl = $(selector).DataTable(settings);
  dTbl.buttons().container().appendTo(`${selector}_wrapper .col-md-6:eq(0)`);
  dTbl.columns.adjust().draw();
}

function findHeaderIndex(tableSelector, headerName) {
  var cols = $(tableSelector + " thead th");
  var i = -1;

  cols.each(function (index) {
    var headerText = $(this).text().toUpperCase();
    if (headerText.includes(headerName)) {
      i = index;
      return false;
    }
  });

  return i;
}

function calculateDifference(tableSelector, resultSelector) {
  var totalDifference = 0;
  var colDrAmt = findHeaderIndex(tableSelector, "DEBIT_AMOUNT");
  var colCrAmt = findHeaderIndex(tableSelector, "CREDIT_AMOUNT");

  $(tableSelector + " .checkOne:checked").each(function () {
    var row = $(this).closest("tr");
    var drAmt = parseFloat(
      row.find(`td:eq(${colDrAmt})`).text().replace(/,/g, "")
    );
    var crAmt = parseFloat(
      row.find(`td:eq(${colCrAmt})`).text().replace(/,/g, "")
    );
    totalDifference += drAmt - crAmt;
  });
  $(resultSelector).text(
    Math.abs(totalDifference)
      .toFixed(2)
      .replace(/\B(?=(\d{3})+(?!\d))/g, ",")
  );
}

function updateButtonState(
  buttonSelector,
  mainResultSelector,
  otherResultSelector
) {
  var res1 = parseFloat(
    $(mainResultSelector)
      .text()
      .replace(/[^0-9.-]+/g, "")
  );
  var res2 = parseFloat(
    $(otherResultSelector)
      .text()
      .replace(/[^0-9.-]+/g, "")
  );

  if (res1 !== 0 && res2 !== 0 && res1 === res2) {
    $(buttonSelector).prop("disabled", false);
  } else {
    $(buttonSelector).prop("disabled", true);
  }
}

function setupCheckboxListener(
  tableSelector,
  mainResultSelector,
  otherResultSelector,
  selectAllSelector,
  btnSelector
) {
  $(tableSelector).on("change", ".checkOne", function () {
    calculateDifference(tableSelector, mainResultSelector);
    updateButtonState(btnSelector, mainResultSelector, otherResultSelector);

    var allChecked =
      $(tableSelector + " .checkOne").length ===
      $(tableSelector + " .checkOne:checked").length;
    $(selectAllSelector).prop("checked", allChecked);
  });

  $(selectAllSelector).on("change", function () {
    var isChecked = $(this).is(":checked");
    $(tableSelector + " .checkOne").prop("checked", isChecked);
    calculateDifference(tableSelector, mainResultSelector);
    updateButtonState(btnSelector, mainResultSelector, otherResultSelector);
  });

  $(tableSelector).on("click", "tbody tr", function (e) {
    if (!$(e.target).closest("td").find(".checkOne").length) {
      var checkbox = $(this).find(".checkOne");
      checkbox.prop("checked", !checkbox.is(":checked")).trigger("change");
    }
  });
}

$(document).ready(function () {
  $("#sumDisplayBS").text("0");
  $("#sumDisplayGL").text("0");
  $("#sumDisplayMatchedGL").text("0");
  $("#sumDisplayMatchedBS").text("0");

  initializeDataTable("#bs", dTblSettings);
  initializeDataTable("#gl", dTblSettings);
  initializeDataTable("#match_gl", dTblSettings);
  initializeDataTable("#match_bs", dTblSettings);

  setupCheckboxListener(
    "#bs",
    "#sumDisplayBS",
    "#sumDisplayGL",
    "#checkAllUmBS",
    "#btn_is_match"
  );
  setupCheckboxListener(
    "#gl",
    "#sumDisplayGL",
    "#sumDisplayBS",
    "#checkAllUmGL",
    "#btn_is_match"
  );
  setupCheckboxListener(
    "#match_bs",
    "#sumDisplayMatchedBS",
    "#sumDisplayMatchedGL",
    "#checkAllMatchedBS",
    "#btn_unmatch"
  );
  setupCheckboxListener(
    "#match_gl",
    "#sumDisplayMatchedGL",
    "#sumDisplayMatchedBS",
    "#checkAllMatchedGL",
    "#btn_unmatch"
  );
});
