$(document).ready(function () {
  $(".checkAll").click(function (e) {
    id = e.target.id;
    if (this.checked) {
      $(".check" + id).each(function () {
        this.checked = true;
      });
    } else {
      $(".check" + id).each(function () {
        this.checked = false;
      });
    }
  });
  var table = $("table.main-tbl").DataTable({
    dom: "BQlfrtip",
    stateSave: true,
    lengthMenu: [
      [10, 25, 50, 100, -1],
      ["10 rows", "25 rows", "50 rows", "100 rows", "Show all"],
    ],
    lengthChange: false,
    responsive: true,
    colReorder: true,
    language: {
      buttons: {
        colvisRestore: "RESET COLUMN",
      },
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
            //return $('table#main').DataTable().column(idx).visible();
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
            //return $('table#main').DataTable().column(idx).visible();
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
  });
  table.buttons().container().appendTo(".main-tbl_wrapper .col-md-6:eq(0)");
  table.columns.adjust().draw();
  // DataTable.SearchBuilder(table, {});
  // table.searchBuilder.container().prependTo(table.table().container());

  $("#revOfAjeGl").popover({ trigger: "manual", placement: "bottom" });
  $("#revOfAjeBs").popover({ trigger: "manual", placement: "bottom" });
});

function validateOnlyNums(input) {
  // Regex to allow numbers with optional commas and a decimal point
  const inputRegex = /^-?\d+(\.\d*)?(,\d{3})*(\.\d+)?$/;

  // Get the input value without commas for validation
  const isValid = inputRegex.test(input.value.replace(/,/g, ""));

  // Handle UI elements
  const submitButton = document.querySelector("#" + input.id + "Btn");

  console.log(isValid);

  if (input.value.trim() === "") {
    submitButton.disabled = true;
    $(input).popover("hide");
    input.style.borderColor = "";
    return;
  }

  if (isValid || input.value === "") {
    $(input).popover("hide");
    input.style.borderColor = "";
    submitButton.disabled = false;
  } else {
    $(input).popover("show");
    input.style.borderColor = "red";
    submitButton.disabled = true;
  }
}

// Function to format input with commas when the user leaves the input field
function formatWithCommasOnBlur(input) {
  let value = input.value.replace(/,/g, "");

  // Only format if it's a valid number and not empty
  if (value && !isNaN(value) && value.indexOf('.') !== value.length - 1) {
    const parts = value.split(".");
    parts[0] = Number(parts[0]).toLocaleString("en-US");  // Add commas to the integer part
    input.value = parts.join(".");  // Join integer and decimal parts
  }
}

// Event listeners to manage input and blur events
document.querySelectorAll('input.currency-input').forEach(input => {
  // Validate while typing (optional)
  input.addEventListener('input', () => validateOnlyNums(input));

  // Format with commas when the input loses focus
  input.addEventListener('blur', () => formatWithCommasOnBlur(input));
});
