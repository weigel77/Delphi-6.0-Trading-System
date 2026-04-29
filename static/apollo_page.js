(function () {
    function syncDateFields() {
        var querySelect = document.getElementById("query_type");
        var singleDateGroup = document.getElementById("single-date-group");
        var startDateGroup = document.getElementById("start-date-group");
        var endDateGroup = document.getElementById("end-date-group");

        if (!querySelect || !singleDateGroup || !startDateGroup || !endDateGroup) {
            return;
        }

        var selectedOption = querySelect.options[querySelect.selectedIndex];
        var dateMode = selectedOption.dataset.dateMode;

        singleDateGroup.style.display = dateMode === "single" ? "flex" : "none";
        startDateGroup.style.display = dateMode === "range" ? "flex" : "none";
        endDateGroup.style.display = dateMode === "range" ? "flex" : "none";
    }

    function attachTransferFormHandlers() {
        var forms = document.querySelectorAll("[data-disable-submit-buttons]");

        forms.forEach(function (form) {
            form.addEventListener("submit", function () {
                form.querySelectorAll("button").forEach(function (button) {
                    button.disabled = true;
                });
            });
        });
    }

    function renderApolloCharts() {
        var chartShells = document.querySelectorAll("[data-apollo-chart]");

        chartShells.forEach(function (shell) {
            var canvas = shell.querySelector(".apollo-chart-canvas");
            if (!canvas) {
                return;
            }

            var payload = JSON.parse(shell.dataset.apolloChart || "{}");
            if (!payload.available || !Array.isArray(payload.points) || payload.points.length === 0) {
                canvas.innerHTML = '<p class="subtle">Chart data is unavailable.</p>';
                return;
            }

            var width = 1240;
            var height = 420;
            var padding = { top: 18, right: 64, bottom: 34, left: 54 };
            var plotWidth = width - padding.left - padding.right;
            var plotHeight = height - padding.top - padding.bottom;
            var allPrices = [];

            payload.points.forEach(function (point) {
                allPrices.push(point.high, point.low, point.close, point.open);
                if (point.ema8 !== null) {
                    allPrices.push(point.ema8);
                }
                if (point.ema21 !== null) {
                    allPrices.push(point.ema21);
                }
            });

            var minPrice = Math.min.apply(null, allPrices);
            var maxPrice = Math.max.apply(null, allPrices);
            var pricePadding = Math.max((maxPrice - minPrice) * 0.08, 2);
            var chartMin = minPrice - pricePadding;
            var chartMax = maxPrice + pricePadding;
            var candleWidth = Math.max(Math.min((plotWidth / payload.points.length) * 0.58, 10), 4);

            function xForIndex(index) {
                return padding.left + (plotWidth * (index + 0.5)) / payload.points.length;
            }

            function yForPrice(price) {
                return padding.top + ((chartMax - price) / (chartMax - chartMin || 1)) * plotHeight;
            }

            function createSvgElement(tag) {
                return document.createElementNS("http://www.w3.org/2000/svg", tag);
            }

            var svg = createSvgElement("svg");
            svg.setAttribute("viewBox", "0 0 " + width + " " + height);
            svg.setAttribute("class", "apollo-chart-svg");

            var background = createSvgElement("rect");
            background.setAttribute("x", "0");
            background.setAttribute("y", "0");
            background.setAttribute("width", String(width));
            background.setAttribute("height", String(height));
            background.setAttribute("rx", "14");
            background.setAttribute("fill", "#f9fbff");
            svg.appendChild(background);

            for (var step = 0; step <= 4; step += 1) {
                var price = chartMin + ((chartMax - chartMin) * step) / 4;
                var y = yForPrice(price);
                var grid = createSvgElement("line");
                grid.setAttribute("x1", String(padding.left));
                grid.setAttribute("x2", String(width - padding.right));
                grid.setAttribute("y1", String(y));
                grid.setAttribute("y2", String(y));
                grid.setAttribute("stroke", "#dbe3ef");
                grid.setAttribute("stroke-dasharray", "4 4");
                svg.appendChild(grid);

                var gridLabel = createSvgElement("text");
                gridLabel.setAttribute("x", String(width - padding.right + 8));
                gridLabel.setAttribute("y", String(y + 4));
                gridLabel.setAttribute("fill", "#5b6f88");
                gridLabel.setAttribute("font-size", "12");
                gridLabel.textContent = price.toFixed(2);
                svg.appendChild(gridLabel);
            }

            payload.points.forEach(function (point, index) {
                var x = xForIndex(index);
                var candleColor = point.close >= point.open ? "#146c43" : "#8a1c28";

                var wick = createSvgElement("line");
                wick.setAttribute("x1", String(x));
                wick.setAttribute("x2", String(x));
                wick.setAttribute("y1", String(yForPrice(point.high)));
                wick.setAttribute("y2", String(yForPrice(point.low)));
                wick.setAttribute("stroke", candleColor);
                wick.setAttribute("stroke-width", "1.4");
                svg.appendChild(wick);

                var body = createSvgElement("rect");
                var bodyTop = yForPrice(Math.max(point.open, point.close));
                var bodyBottom = yForPrice(Math.min(point.open, point.close));
                body.setAttribute("x", String(x - candleWidth / 2));
                body.setAttribute("y", String(bodyTop));
                body.setAttribute("width", String(candleWidth));
                body.setAttribute("height", String(Math.max(bodyBottom - bodyTop, 1.5)));
                body.setAttribute("fill", candleColor);
                body.setAttribute("opacity", "0.85");
                svg.appendChild(body);
            });

            [
                { key: "ema8", color: "#8b5cf6", width: 1.8 },
                { key: "ema21", color: "#f59e0b", width: 1.8 }
            ].forEach(function (overlay) {
                var pathPoints = payload.points
                    .map(function (point, index) {
                        if (point[overlay.key] === null) {
                            return null;
                        }
                        return xForIndex(index) + "," + yForPrice(point[overlay.key]);
                    })
                    .filter(Boolean);

                if (!pathPoints.length) {
                    return;
                }

                var polyline = createSvgElement("polyline");
                polyline.setAttribute("points", pathPoints.join(" "));
                polyline.setAttribute("fill", "none");
                polyline.setAttribute("stroke", overlay.color);
                polyline.setAttribute("stroke-width", String(overlay.width));
                polyline.setAttribute("stroke-linejoin", "round");
                polyline.setAttribute("stroke-linecap", "round");
                svg.appendChild(polyline);
            });

            var priceLine = createSvgElement("line");
            priceLine.setAttribute("x1", String(padding.left));
            priceLine.setAttribute("x2", String(width - padding.right));
            priceLine.setAttribute("y1", String(yForPrice(payload.current_price)));
            priceLine.setAttribute("y2", String(yForPrice(payload.current_price)));
            priceLine.setAttribute("stroke", "#111827");
            priceLine.setAttribute("stroke-dasharray", "6 5");
            priceLine.setAttribute("stroke-width", "1.3");
            svg.appendChild(priceLine);

            var priceLabel = createSvgElement("text");
            priceLabel.setAttribute("x", String(width - padding.right + 8));
            priceLabel.setAttribute("y", String(yForPrice(payload.current_price) - 6));
            priceLabel.setAttribute("fill", "#111827");
            priceLabel.setAttribute("font-size", "12");
            priceLabel.textContent = "Last " + payload.current_price.toFixed(2);
            svg.appendChild(priceLabel);

            [0, Math.floor(payload.points.length / 2), payload.points.length - 1]
                .filter(function (value, position, array) {
                    return array.indexOf(value) === position;
                })
                .forEach(function (index) {
                    var point = payload.points[index];
                    var label = createSvgElement("text");
                    var anchor = "middle";

                    if (index === 0) {
                        anchor = "start";
                    } else if (index === payload.points.length - 1) {
                        anchor = "end";
                    }

                    label.setAttribute("x", String(xForIndex(index)));
                    label.setAttribute("y", String(height - 10));
                    label.setAttribute("fill", "#5b6f88");
                    label.setAttribute("font-size", "12");
                    label.setAttribute("text-anchor", anchor);
                    label.textContent = point.label;
                    svg.appendChild(label);
                });

            canvas.innerHTML = "";
            canvas.appendChild(svg);
        });
    }

    document.addEventListener("DOMContentLoaded", function () {
        var querySelect = document.getElementById("query_type");

        if (querySelect) {
            querySelect.addEventListener("change", syncDateFields);
        }

        syncDateFields();
        attachTransferFormHandlers();
        renderApolloCharts();
    });
}());