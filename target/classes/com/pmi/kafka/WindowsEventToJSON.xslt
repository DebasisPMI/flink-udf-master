<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:event="http://schemas.microsoft.com/win/2004/08/events/event"
                xmlns:json="http://xml.kafka.pmi.com/2025/json"
>

    <xsl:output method="xml" encoding="utf-8"/>

    <xsl:template match="event:Provider" mode="system">
        <xsl:if test="@Name">
            <json:property name="ProviderName">
                <json:string>
                    <xsl:value-of select="@Name"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@Guid">
            <json:property name="ProviderGuid">
                <json:string>
                    <xsl:value-of select="@Guid"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@EventSourceName">
            <json:property name="Source">
                <json:string>
                    <xsl:value-of select="@EventSourceName"/>
                </json:string>
            </json:property>
        </xsl:if>
    </xsl:template>

    <xsl:template match="event:EventID" mode="system">
        <json:property name="EventID">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Version" mode="system">
        <json:property name="EventVersion">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Level" mode="system">
        <json:property name="EventLevel">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Task" mode="system">
        <json:property name="EventTask">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Opcode" mode="system">
        <json:property name="EventOpcode">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Keywords" mode="system">
        <json:property name="EventKeywords">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:TimeCreated" mode="system">
        <json:property name="TimeGenerated">
            <json:string>
                <xsl:value-of select="@SystemTime"/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:EventRecordID" mode="system">
        <json:property name="EventRecordID">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Correlation" mode="system">
        <xsl:if test="@ActivityID">
            <json:property name="ActivityID">
                <json:string>
                    <xsl:value-of select="@ActivityID"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@RelatedActivityID">
            <json:property name="RelatedActivityID">
                <json:string>
                    <xsl:value-of select="@RelatedActivityID"/>
                </json:string>
            </json:property>
        </xsl:if>
    </xsl:template>

    <xsl:template match="event:Execution" mode="system">
        <xsl:if test="@ProcessID">
            <json:property name="ExecutionProcessID">
                <json:string>
                    <xsl:value-of select="@ProcessID"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@ThreadID">
            <json:property name="ExecutionThreadID">
                <json:string>
                    <xsl:value-of select="@ThreadID"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@ProcessorID">
            <json:property name="ExecutionProcessorID">
                <json:string>
                    <xsl:value-of select="@ProcessorID"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@SessionID">
            <json:property name="ExecutionSessionID">
                <json:string>
                    <xsl:value-of select="@SessionID"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@KernelTime">
            <json:property name="ExecutionKernelTime">
                <json:string>
                    <xsl:value-of select="@KernelTime"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@UserTime">
            <json:property name="ExecutionUserTime">
                <json:string>
                    <xsl:value-of select="@UserTime"/>
                </json:string>
            </json:property>
        </xsl:if>
        <xsl:if test="@ProcessorTime">
            <json:property name="ExecutionProcessorTime">
                <json:string>
                    <xsl:value-of select="@ProcessorTime"/>
                </json:string>
            </json:property>
        </xsl:if>
    </xsl:template>

    <xsl:template match="event:Channel" mode="system">
        <json:property name="Channel">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Computer" mode="system">
        <json:property name="Computer">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Container" mode="system">
        <json:property name="Container">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:Security" mode="system">
        <xsl:if test="@UserID">
            <json:property name="UserID">
                <json:string>
                    <xsl:value-of select="@UserID"/>
                </json:string>
            </json:property>
        </xsl:if>
        <json:property name="Security">
            <json:string>
                <xsl:value-of select="."/>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:*" mode="system">
    </xsl:template>

    <xsl:template match="event:System">
        <xsl:apply-templates mode="system" select="*"/>
    </xsl:template>

    <!--
    <xsl:template match="event:EventData">
        <json:object>
            <xsl:for-each select="event:Data">
                <json:property>
                    <xsl:attribute name="name">
                        <xsl:value-of select="@Name" />
                    </xsl:attribute>
                    <json:string>
                        <xsl:value-of select="text()" />
                    </json:string>
                </json:property>
            </xsl:for-each>
        </json:object>
    </xsl:template>
    -->

    <xsl:template match="event:EventData">
        <json:property name="EventData">
            <json:string>
                <xsl:call-template name="serialize">
                    <xsl:with-param name="node" select="."/>
                </xsl:call-template>
            </json:string>
        </json:property>
    </xsl:template>

    <xsl:template match="event:RenderingInfo">
    </xsl:template>

    <xsl:template match="event:Event">
        <json:object>
            <xsl:apply-templates select="*"/>
        </json:object>
    </xsl:template>

    <!--
    For the root element select its only accepted child 'Event'.
    -->
    <xsl:template match="/">
        <xsl:apply-templates select="event:Event"/>
    </xsl:template>

    <!--
    Drop any unmatched content.
    -->
    <xsl:template match="@*|node()">
    </xsl:template>

    <!-- Recursive serializer -->
    <xsl:template name="serialize">
        <xsl:param name="node"/>
        <xsl:text>&lt;</xsl:text>
        <xsl:value-of select="name($node)"/>
        <xsl:for-each select="$node/@*">
            <xsl:text> </xsl:text>
            <xsl:value-of select="name()"/>
            <xsl:text>="</xsl:text>
            <xsl:value-of select="."/>
            <xsl:text>"</xsl:text>
        </xsl:for-each>
        <xsl:choose>
            <xsl:when test="$node/node()">
                <xsl:text>&gt;</xsl:text>
                <xsl:for-each select="$node/node()">
                    <xsl:choose>
                        <xsl:when test="self::text()">
                            <xsl:value-of select="."/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:call-template name="serialize">
                                <xsl:with-param name="node" select="."/>
                            </xsl:call-template>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:for-each>
                <xsl:text>&lt;/</xsl:text>
                <xsl:value-of select="name($node)"/>
                <xsl:text>&gt;</xsl:text>
            </xsl:when>
            <xsl:otherwise>
                <xsl:text>/&gt;</xsl:text>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

</xsl:stylesheet>
