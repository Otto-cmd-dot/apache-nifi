import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.math.BigDecimal

flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->

    def text = inputStream.getText(StandardCharsets.UTF_8.name())
    def json = new JsonSlurper().parseText(text)

    // --- Helper Transformation Functions ---


    def convertConversionFactorToDecimal155 = { val, defaultVal = new BigDecimal("0.00000") ->
        try {
            if (val == null || val.toString().trim().isEmpty()) {
                return defaultVal.setScale(5, BigDecimal.ROUND_HALF_UP)
            }
            def decimal = new BigDecimal(val.toString().trim()).setScale(5, BigDecimal.ROUND_HALF_UP)
            return decimal < BigDecimal.ONE ? decimal : defaultVal.setScale(5, BigDecimal.ROUND_HALF_UP) // reject ≥ 1.0
        } catch (Exception e) {
            return defaultVal.setScale(5, BigDecimal.ROUND_HALF_UP)
        }
    }


    // --- Process each item ---
    json.each { item ->
        item.name = item.name ?: "N/A"
        item.uom_type = item.uom_type ?: "N/A"
        item.conversion_factor = convertConversionFactorToDecimal155(item.conversion_factor)
        item.base_uom = item.base_uom ?: "N/A"
        item.active = item.active

        item.is_current = 1
        item.source_system_code = item.source_system_code
    }

    def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(json))
    outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
