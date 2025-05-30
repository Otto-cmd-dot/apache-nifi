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
    def transformName = { String name ->
        if (name == null) {
            return "N/A"
        } else {
            return name.toUpperCase()
        }
    }

    def transformCode = { String code ->
        if (code == null) {
            return "N/A"
        } else {
            return code.toUpperCase()
        }
    }

    def convertToDecimal96 = { val ->
        try {
            return (val != null && val.toString().isNumber()) ? new BigDecimal(val).setScale(2, BigDecimal.ROUND_HALF_UP) : new BigDecimal("0.00")
        } catch (e) {
            return new BigDecimal("0.00")
        }
    }


    // --- Process each item ---
    json.each { item ->
        item.id = item.id
        item.name = transformName(item.name)
        item.active = item.active
        item.code = transformCode(item.code)
        item.warehouse_address = item.warehouse_address ?: "N/A"
        item.warehouse_province = item.warehouse_province ?: "N/A"
        item.warehouse_district = item.warehouse_district ?: "N/A"
        item.warehouse_commune = item.warehouse_commune ?: "N/A"
        item.warehouse_village = item.warehouse_village ?: "N/A"
        item.warehouse_postcode = item.warehouse_postcode ?: "N/A"
        item.latitude = convertToDecimal96(item.latitude)
        item.longitude = convertToDecimal96(item.longitude)
        item.is_current = 1
        item.source_system_code = item.source_system_code ?: 1
        

    }

    def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(json))
    outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
