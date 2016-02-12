# Sitecore.Support.96688
The `DataUri.Equals` method might incorrectly determine if two objects have the same value.

## Main

This repository contains Sitecore Patch #96688, which replaces the default `onPublishEndAsync` index update strategy with a custom one that uses a fixed `DataUri.Equals` method.

## Deployment

To apply the patch perform the following steps on both CM and CD servers:

1. Place the `Sitecore.Support.96688.dll` assembly into the `\bin` directory.
2. Place the `z.Sitecore.Support.96688.config` file into the `\App_Config\Include` directory.

## Content 

Sitecore Patch includes the following files:

1. `\bin\Sitecore.Support.96688.dll`
2. `\App_Config\Include\z.Sitecore.Support.96688.config`
