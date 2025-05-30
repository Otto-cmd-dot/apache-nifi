import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.math.BigDecimal
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

// Main entry point for NiFi ExecuteGroovyScript processor
flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, new StreamCallback() {

    // Constants and utilities scoped to StreamCallback instance
    final ZoneId ZONE_ID = ZoneId.of("Asia/Bangkok")
    final DateTimeFormatter DATE_KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd")
    final BigDecimal SENTINEL_DECIMAL = new BigDecimal("-1.00").setScale(2, RoundingMode.HALF_UP)
    final Map<String, BigDecimal> EXCHANGE_RATES = [
        'USD': BigDecimal.ONE,
        'KHR': new BigDecimal("0.00025")
    ]

    // Converts epoch millis or string to date key integer (yyyyMMdd)
    Integer formatDateFieldToDateKey(def val) {
        try {
            String rawStr = val?.toString()
            if (rawStr?.isLong()) {
                def date = Instant.ofEpochMilli(rawStr.toLong()).atZone(ZONE_ID).toLocalDate()
                return Integer.parseInt(date.format(DATE_KEY_FORMATTER))
            }
        } catch (ignored) {}
        return 99991231
    }

    // Converts input to BigDecimal scaled to 2 decimals or returns default/sentinel
    BigDecimal formatToDecimal102(def input, BigDecimal defaultVal = null) {
        if (input == null) return defaultVal ?: SENTINEL_DECIMAL
        try {
            BigDecimal value = new BigDecimal(input.toString().trim()).setScale(2, RoundingMode.HALF_UP)
            // Limit precision: max 8 digits before decimal
            if ((value.precision() - value.scale()) <= 8) {
                return value
            }
            return defaultVal ?: SENTINEL_DECIMAL
        } catch (Exception e) {
            return defaultVal ?: SENTINEL_DECIMAL
        }
    }

    // Safely parse int or fallback to -1
    Integer toIntegerSafe(def val) {
        try {
            if (val != null && val.toString().isNumber()) {
                return Integer.parseInt(val.toString())
            }
        } catch (Exception ignored) {}
        return -1
    }

    // Returns string or "-1" if null/empty
    String safeStringFallback(def val) {
        if (val == null || val.toString().trim().isEmpty()) {
            return "-1"
        }
        return val.toString()
    }

    // Normalize discount types
    String standardizeDiscountType(String type) {
        switch ((type ?: "").toLowerCase()) {
            case "fixed": return "Fixed"
            case "percentage": return "Percent"
            default: return "N/A"
        }
    }

    // Obtain exchange rate with default fallback
    BigDecimal deriveCurrencyExchangeRate(String currency) {
        return EXCHANGE_RATES.getOrDefault((currency ?: "USD").toUpperCase(), BigDecimal.ONE)
    }

    // Convert value to base currency applying exchange rate
    BigDecimal convertToBaseCurrency(BigDecimal val, BigDecimal rate) {
        if (val == null || val.compareTo(SENTINEL_DECIMAL) == 0) {
            return SENTINEL_DECIMAL
        }
        return val.multiply(rate).setScale(2, RoundingMode.HALF_UP)
    }

    // Calculate gross amount (unitPrice * qty)
    BigDecimal calculateGrossAmount(BigDecimal unitPrice, int qty) {
        if (qty == -1 || unitPrice == null || unitPrice.compareTo(SENTINEL_DECIMAL) == 0) {
            return SENTINEL_DECIMAL
        }
        return formatToDecimal102(unitPrice.multiply(new BigDecimal(qty)))
    }

    // Calculate discount on order line based on type
    BigDecimal calculateOriginalOrderLineDiscount(String discountType, BigDecimal grossAmount, BigDecimal discount) {
        if (grossAmount == null || discount == null || grossAmount.compareTo(SENTINEL_DECIMAL) == 0 || discount.compareTo(SENTINEL_DECIMAL) == 0 || discountType == 'N/A') {
            return SENTINEL_DECIMAL
        }
        switch (discountType) {
            case "Fixed": 
                return formatToDecimal102(discount)
            case "Percent":
                return formatToDecimal102(grossAmount.multiply(discount.divide(new BigDecimal("100"))))
            default:
                return SENTINEL_DECIMAL
        }
    }

    // Calculate net amount after discount
    BigDecimal calculateOriginalExtendedOrderLineNetAmount(BigDecimal unitPrice, int qty, BigDecimal discount) {
        if (unitPrice == null || discount == null || unitPrice.compareTo(SENTINEL_DECIMAL) == 0 || discount.compareTo(SENTINEL_DECIMAL) == 0 || qty == -1) {
            return SENTINEL_DECIMAL
        }
        return formatToDecimal102(unitPrice.multiply(new BigDecimal(qty)).subtract(discount))
    }

    // Calculate overall order discount based on type
    BigDecimal calculateOriginalOrderDiscount(String discountType, BigDecimal discountValue, BigDecimal totalAmount) {
        if (discountValue == null || totalAmount == null || discountValue.compareTo(SENTINEL_DECIMAL) == 0 || totalAmount.compareTo(SENTINEL_DECIMAL) == 0) {
            return SENTINEL_DECIMAL
        }
        if (!discountType || discountType.trim().toLowerCase() == "n/a") {
            return SENTINEL_DECIMAL
        }
        switch (discountType.toLowerCase()) {
            case "fixed":
                return formatToDecimal102(discountValue)
            case "percent":
                return formatToDecimal102(totalAmount.multiply(discountValue.divide(new BigDecimal("100"))))
            default:
                return SENTINEL_DECIMAL
        }
    }

    // Main transformation
    @Override
    void process(InputStream inputStream, OutputStream outputStream) throws IOException {
        def json = new JsonSlurper().parseText(inputStream.getText(StandardCharsets.UTF_8.name()))

        json.each { item ->

            // Normalize and fallback values early
            item.with {
                order_id = order_id ?: -1
                currency_id = safeStringFallback(currency_id)
                customer_id = safeStringFallback(customer_id)
                warehouse_id = safeStringFallback(warehouse_id)
                employee_id = safeStringFallback(employee_id)
                product_id = safeStringFallback(product_id)
                product_uom = safeStringFallback(product_uom)
                company_branch_id = safeStringFallback(company_branch_id)

                request_delivery_date = formatDateFieldToDateKey(request_delivery_date)
                date_order_id = formatDateFieldToDateKey(date_order_id)
                invoice_date = formatDateFieldToDateKey(invoice_date)
                expiration_date = formatDateFieldToDateKey(expiration_date)

                order_name = order_name?.toUpperCase()
                sale_order_code = sale_order_code?.toUpperCase() ?: "N/A"
                order_discount_type = standardizeDiscountType(order_discount_type)
                order_line_discount_type = standardizeDiscountType(order_line_discount_type)

                // Normalize all decimals upfront
                original_order_untaxed_amount = formatToDecimal102(original_order_untaxed_amount)
                original_order_tax_amount = formatToDecimal102(original_order_tax_amount)
                original_order_total_discount = formatToDecimal102(original_order_total_discount)
                original_total_order_amount = formatToDecimal102(original_total_order_amount)
                original_order_line_discount = formatToDecimal102(original_order_line_discount)
                original_order_line_tax_amount = formatToDecimal102(original_order_line_tax_amount)
                original_unit_price = formatToDecimal102(original_unit_price)
                original_order_line_price_subtotal = formatToDecimal102(original_order_line_price_subtotal)
                original_order_discount = formatToDecimal102(original_order_discount)

                quantity = toIntegerSafe(quantity)
                is_downpayment = is_downpayment
                payment_term = payment_term ?: "N/A"
                pricelist = pricelist ?: "N/A"
                business_unit = business_unit ?: "N/A"

                // Currency and conversion setup
                base_currency = "USD"
                BigDecimal exchangeRate = deriveCurrencyExchangeRate(current_currency)
                exchange_rate = exchangeRate

                unit_price = convertToBaseCurrency(original_unit_price, exchangeRate)
                order_untaxed_amount = convertToBaseCurrency(original_order_untaxed_amount, exchangeRate)
                order_tax_amount = convertToBaseCurrency(original_order_tax_amount, exchangeRate)
                order_total_discount = convertToBaseCurrency(original_order_total_discount, exchangeRate)
                total_order_amount = convertToBaseCurrency(original_total_order_amount, exchangeRate)

                extended_order_line_gross_amount = calculateGrossAmount(unit_price, quantity)

                original_order_line_discount = calculateOriginalOrderLineDiscount(order_line_discount_type, extended_order_line_gross_amount, original_order_line_discount)
                order_line_discount = convertToBaseCurrency(original_order_line_discount, exchangeRate)

                order_line_tax_amount = convertToBaseCurrency(original_order_line_tax_amount, exchangeRate)
                order_line_price_subtotal = convertToBaseCurrency(original_order_line_price_subtotal, exchangeRate)

                original_extended_order_line_net_amount = calculateOriginalExtendedOrderLineNetAmount(original_unit_price, quantity, original_order_line_discount)
                extended_order_line_net_amount = convertToBaseCurrency(original_extended_order_line_net_amount, exchangeRate)

                original_order_discount = calculateOriginalOrderDiscount(order_discount_type, original_order_discount, original_total_order_amount)
                order_discount = convertToBaseCurrency(original_order_discount, exchangeRate)

                is_current = 1
                source_system_code = source_system_code
            }
        }

        // Output transformed JSON
        def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(json))
        outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))
    }
})

session.transfer(flowFile, REL_SUCCESS)
