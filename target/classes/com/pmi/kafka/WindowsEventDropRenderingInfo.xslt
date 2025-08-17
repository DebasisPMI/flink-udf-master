<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:event="http://schemas.microsoft.com/win/2004/08/events/event"
>

    <xsl:output method="xml" indent="no" encoding="utf-8"/>

    <!-- for this element ignore the matching -->
    <xsl:template match="event:RenderingInfo"/>

    <!-- for any other node (element or attribute) copy it directly-->
    <xsl:template match="@*|node()">
        <!--
        emit a direct clone of the currently matched node or attribute
        -->
        <xsl:copy>
            <!--
            and then do the pattern matching for all its children: nodes, attributes, processing instructions, text,
            cdata, comments, etc.
            -->
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
