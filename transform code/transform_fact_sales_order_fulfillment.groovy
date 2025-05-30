import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->

    def inputText = inputStream.getText(StandardCharsets.UTF_8.name())
    def records = new JsonSlurper().parseText(inputText)

    // -------------------------
    //      Helper Functions
    // -------------------------

    def SENTINEL_DECIMAL = new BigDecimal("-1.00")

    def deriveExchangeRate = { currency ->
        def ratesToUSD = ['USD': 1.0, 'KHR': 0.00025] // 1 / 4100.0
        return ratesToUSD.getOrDefault(currency?.toUpperCase(), 1.0)
    }


    def fallbackString = { val, fallback = "-1" ->
        return (val == null || val.toString().trim().isEmpty()) ? fallback : val.toString()
    }

    def toIntegerSafe = { val ->
        try {
            return val?.toString()?.isNumber() ? val.toInteger() : -1
        } catch (e) {
            return -1
        }
    }

    def toDecimal = { val, defaultVal = SENTINEL_DECIMAL ->
        try {
            def bd = new BigDecimal(val.toString().trim()).setScale(2, RoundingMode.HALF_UP)
            return bd.precision() - bd.scale() <= 15 ? bd : defaultVal
        } catch (e) {
            return defaultVal
        }
    }

    // Safe multiplication
    def safeMultiply = { a, b ->
        try {
            def bd1 = (a instanceof BigDecimal) ? a : new BigDecimal(a.toString())
            def bd2 = (b instanceof BigDecimal) ? b : new BigDecimal(b.toString())
            return bd1.multiply(bd2).setScale(2, RoundingMode.HALF_UP)
        } catch (e) {
            return SENTINEL_DECIMAL
        }
    }

    // Check for sentinel (-1.00) before multiplying
    def safeMultiplyUnlessSentinel = { original, rate ->
        return (original == SENTINEL_DECIMAL) ? SENTINEL_DECIMAL : safeMultiply(original, rate)
    }


    def formatDateToKey = { epochMillis ->
        try {
            if (epochMillis == null || !epochMillis.toString().isLong()) return 99991231
            def instant = Instant.ofEpochMilli(epochMillis.toLong())
            return instant.atZone(ZoneId.of("Asia/Bangkok"))
                         .toLocalDate()
                         .format(DateTimeFormatter.ofPattern("yyyyMMdd"))
                         .toInteger()
        } catch (e) {
            return 99991231
        }
    }

    def mapInvoiceStatus = { status ->
        switch (status?.toLowerCase()) {
            case "no": return "Quotation"
            case "to invoice": return "To-Invoice"
            case "invoiced": return "Invoiced"
            default: return "N/A"
        }
    }

    def mapOrderState = { state ->
        switch (state?.toLowerCase()) {
            case "draft": return "Draft"
            case "sent": return "Quotation-Sent"
            case "waiting": return "Waiting"
            case "sale": return "Sale"
            case "done": return "Invoiced"
            case "cancel": return "Cancel"
            default: return "N/A"
        }
    }

    def mapShippingPolicy = { policy ->
        switch (policy?.toLowerCase()) {
            case "direct": return "ASAP"
            case "one": return "All-Products"
            default: return "N/A"
        }
    }

    // -------------------------
    //      Main Processing
    // -------------------------

    records.each { record ->
        // Natural key fields
        record.currency_id        = fallbackString(record.currency_id)
        record.customer_id        = fallbackString(record.customer_id)
        record.warehouse_id       = fallbackString(record.warehouse_id)
        record.employee_id        = fallbackString(record.employee_id)
        record.product_id         = fallbackString(record.product_id)
        record.product_uom        = fallbackString(record.product_uom)
        record.company_branch_id  = fallbackString(record.company_branch_id)

        // Core text fields
        record.order_name         = fallbackString(record.order_name?.toUpperCase(), "N/A")
        record.order_line_name    = fallbackString(record.order_line_name?.toUpperCase(), "N/A")
        record.customer_note      = fallbackString(record.customer_note, "N/A")
        record.payment_term       = fallbackString(record.payment_term, "N/A")
        record.pricelist          = fallbackString(record.pricelist, "N/A")
        record.business_unit      = fallbackString(record.business_unit, "N/A")

        // Status & policy mappings
        record.invoice_status     = mapInvoiceStatus(record.invoice_status)
        record.order_state        = mapOrderState(record.order_state)
        record.shipping_policy    = mapShippingPolicy(record.shipping_policy)

        // Integers
        record.order_id           = toIntegerSafe(record.order_id)
        record.quantity           = toIntegerSafe(record.quantity)
        record.qty_delivered      = toIntegerSafe(record.qty_delivered)
        record.qty_invoiced       = toIntegerSafe(record.qty_invoiced)
        record.qty_returned       = toIntegerSafe(record.qty_returned)

        // Dates
        record.request_delivery_date  = formatDateToKey(record.request_delivery_date)
        record.date_order_id          = formatDateToKey(record.date_order_id)
        record.actual_ship_date       = formatDateToKey(record.actual_ship_date)
        record.invoice_date           = formatDateToKey(record.invoice_date)
        record.expiration_date        = formatDateToKey(record.expiration_date)
        record.return_date            = formatDateToKey(record.return_date)

        // Exchange
        record.local_currency         = fallbackString(record.local_currency, "USD")
        record.exchange_rate          = deriveExchangeRate(record.local_currency)

        // Decimal amounts
        record.original_total_order_amount        = toDecimal(record.original_total_order_amount)
        record.original_order_line_price_subtotal = toDecimal(record.original_order_line_price_subtotal)

        // Derived values (sentinel check built-in)
        record.total_order_amount = safeMultiplyUnlessSentinel(
            record.original_total_order_amount, record.exchange_rate)

        record.order_line_price_subtotal = safeMultiplyUnlessSentinel(
            record.original_order_line_price_subtotal, record.exchange_rate)

        // Metadata
        record.is_current           = 1
        record.source_system_code  = toIntegerSafe(record.source_system_code) ?: 1
    }

    def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(records))
    outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))

} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
