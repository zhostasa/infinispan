<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="xml" indent="yes"/>

    <xsl:param name="slot-value"/>
    <xsl:param name="module-name"/>

    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="//*[local-name() = 'dependencies']/*[local-name()= 'module'][@name=$module-name]">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
            <xsl:attribute name="slot">
                <xsl:value-of select="$slot-value"/>
            </xsl:attribute>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
